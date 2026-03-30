package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/xuri/excelize/v2"
)

const (
	numWorkers     = 10
	outputFile     = "merged_output.csv"
	problemLogFile = "problem_rows.log"

	// unzipLimit caps the decompressed size per xlsx to guard against zip bombs.
	unzipLimit = 500 * 1024 * 1024 // 500 MB
)

func main() {
	start := time.Now()

	baseDir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	outPath := filepath.Join(baseDir, outputFile)
	problemLogPath := filepath.Join(baseDir, problemLogFile)

	// Open problem-row log file (truncate on each run).
	problemLog, err := newProblemLogger(problemLogPath)
	if err != nil {
		log.Fatalf("cannot create problem log: %v", err)
	}
	defer problemLog.close()

	if _, err := os.Stat(outPath); err == nil {
		log.Fatalf("output file already exists: %s\n  delete or rename it before running again.", outPath)
	}

	files, err := findXlsxFiles(baseDir, outPath)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d xlsx files\n\n", len(files))
	if len(files) == 0 {
		fmt.Println("Nothing to merge.")
		return
	}

	// Phase 1: merge groups concurrently into temp CSV files
	fmt.Println("=== Phase 1: concurrent group merges ===")
	groups := splitIntoGroups(files, numWorkers)
	tempFiles := runPhase1(baseDir, groups, problemLog)
	defer cleanupTempFiles(tempFiles)

	// Phase 2: concatenate all temp CSVs into one output file
	fmt.Printf("\n=== Phase 2: concatenating %d temp files into %s ===\n", len(tempFiles), outputFile)
	totalRows, err := concatCSVFiles(tempFiles, outPath)
	if err != nil {
		log.Fatalf("phase 2 failed: %v", err)
	}

	problemLog.printSummary()
	fmt.Printf("\nDone in %s — %d data rows written to:\n  %s\n",
		time.Since(start).Round(time.Millisecond), totalRows, outPath)
	if problemLog.count() > 0 {
		fmt.Printf("  %d problem row(s) logged to: %s\n", problemLog.count(), problemLogPath)
	}
}

// findXlsxFiles walks baseDir recursively, returning all .xlsx paths except the output
// file and any temp_group_* files.
func findXlsxFiles(baseDir, excludePath string) ([]string, error) {
	absExclude, _ := filepath.Abs(excludePath)
	var files []string
	err := filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".xlsx" {
			return nil
		}
		absPath, _ := filepath.Abs(path)
		if absPath == absExclude {
			return nil
		}
		if strings.HasPrefix(filepath.Base(path), "temp_group_") {
			return nil
		}
		files = append(files, path)
		return nil
	})
	return files, err
}

func splitIntoGroups(files []string, n int) [][]string {
	if n > len(files) {
		n = len(files)
	}
	groups := make([][]string, n)
	for i, f := range files {
		groups[i%n] = append(groups[i%n], f)
	}
	return groups
}

// problemLogger records rows that could not be parsed, writing each entry to a log file.
type problemLogger struct {
	mu   sync.Mutex
	f    *os.File
	n    int
	logger *log.Logger
}

func newProblemLogger(path string) (*problemLogger, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &problemLogger{
		f:      f,
		logger: log.New(f, "", log.Ldate|log.Ltime),
	}, nil
}

// record logs a single problem row.
//   file    — source xlsx filename
//   sheet   — sheet name
//   rowNum  — 1-based row number within the sheet
//   reason  — description of what went wrong
//   partial — any columns that were partially read (may be nil)
func (pl *problemLogger) record(file, sheet string, rowNum int, reason string, partial []string) {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	pl.n++
	if len(partial) > 0 {
		pl.logger.Printf("[PROBLEM] file=%s sheet=%s row=%d reason=%q partial_cols=%v",
			file, sheet, rowNum, reason, partial)
	} else {
		pl.logger.Printf("[PROBLEM] file=%s sheet=%s row=%d reason=%q",
			file, sheet, rowNum, reason)
	}
}

func (pl *problemLogger) count() int {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	return pl.n
}

func (pl *problemLogger) printSummary() {
	n := pl.count()
	if n > 0 {
		fmt.Printf("\nWARNING: %d row(s) had issues and were skipped — see %s\n", n, problemLogFile)
	}
}

func (pl *problemLogger) close() { pl.f.Close() }

// runPhase1 launches one goroutine per group, each merging its xlsx files into a temp CSV.
func runPhase1(baseDir string, groups [][]string, pl *problemLogger) []string {
	type result struct {
		tempPath string
		rows     int
		err      error
	}

	ch := make(chan result, numWorkers)
	var wg sync.WaitGroup

	for i, group := range groups {
		if len(group) == 0 {
			continue
		}
		wg.Add(1)
		go func(idx int, g []string) {
			defer wg.Done()
			tempPath := filepath.Join(baseDir, fmt.Sprintf("temp_group_%02d.csv", idx))
			fmt.Printf("[worker %02d] processing %d files...\n", idx, len(g))
			rows, err := mergeXlsxToCSV(g, tempPath, pl)
			if err != nil {
				fmt.Printf("[worker %02d] ERROR: %v\n", idx, err)
			} else {
				fmt.Printf("[worker %02d] done — %d rows -> %s\n", idx, rows, filepath.Base(tempPath))
			}
			ch <- result{tempPath, rows, err}
		}(i, group)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	var tempFiles []string
	for r := range ch {
		if r.err == nil {
			tempFiles = append(tempFiles, r.tempPath)
		}
	}
	return tempFiles
}

// mergeXlsxToCSV reads all src xlsx files and appends their rows as tab-separated lines.
// Header is written once; subsequent file headers are skipped.
func mergeXlsxToCSV(srcs []string, dst string, pl *problemLogger) (int, error) {
	f, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	headerWritten := false
	totalRows := 0

	for _, srcPath := range srcs {
		n, err := appendXlsxToCSV(f, srcPath, &headerWritten, pl)
		if err != nil {
			log.Printf("  warning: %s — %v", filepath.Base(srcPath), err)
		}
		totalRows += n
	}
	return totalRows, nil
}

func appendXlsxToCSV(dst *os.File, srcPath string, headerWritten *bool, pl *problemLogger) (int, error) {
	src, err := excelize.OpenFile(srcPath, excelize.Options{
		RawCellValue:   true,
		UnzipSizeLimit: unzipLimit,
	})
	if err != nil {
		return 0, err
	}
	defer src.Close()

	sheets := src.GetSheetList()
	if len(sheets) == 0 {
		return 0, fmt.Errorf("no sheets found")
	}

	written := 0
	fileName := filepath.Base(srcPath)

	for _, sheetName := range sheets {
		rows, err := src.Rows(sheetName)
		if err != nil {
			pl.record(fileName, sheetName, 0, "open sheet: "+err.Error(), nil)
			continue
		}

		rowNum := 0
		isHeaderRow := true

		for rows.Next() {
			rowNum++
			cols, err := rows.Columns()
			if err != nil {
				pl.record(fileName, sheetName, rowNum, err.Error(), cols)
				continue
			}
			if isHeaderRow {
				isHeaderRow = false
				if !*headerWritten {
					if err := writeRow(dst, cols); err != nil {
						rows.Close()
						return written, fmt.Errorf("write header: %w", err)
					}
					*headerWritten = true
				}
				continue
			}
			if len(cols) == 0 {
				continue // skip blank rows silently
			}
			if err := writeRow(dst, cols); err != nil {
				rows.Close()
				return written, fmt.Errorf("write row %d: %w", rowNum, err)
			}
			written++
		}
		if err := rows.Error(); err != nil {
			pl.record(fileName, sheetName, rowNum, "row iterator: "+err.Error(), nil)
		}
		rows.Close()
	}
	return written, nil
}

// writeRow writes a comma-separated row followed by newline.
// Fields containing commas, double-quotes, or newlines are quoted per RFC 4180.
func writeRow(f *os.File, cols []string) error {
	for i, v := range cols {
		if i > 0 {
			if _, err := f.WriteString(","); err != nil {
				return err
			}
		}
		if strings.ContainsAny(v, ",\"\n\r") {
			v = `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
		}
		if _, err := f.WriteString(v); err != nil {
			return err
		}
	}
	_, err := f.WriteString("\n")
	return err
}

// concatCSVFiles concatenates temp CSV files into a single output file.
// The header from the first file is kept; headers from subsequent files are skipped.
func concatCSVFiles(tempFiles []string, outPath string) (int, error) {
	out, err := os.Create(outPath)
	if err != nil {
		return 0, err
	}
	defer out.Close()

	totalRows := 0
	headerWritten := false

	for _, tmpPath := range tempFiles {
		f, err := os.Open(tmpPath)
		if err != nil {
			log.Printf("  warning: cannot open %s: %v", tmpPath, err)
			continue
		}

		rows, err := copyCSV(f, out, !headerWritten)
		f.Close()
		// Mark header written regardless of error: if any bytes were flushed to out
		// before the error, a second header from the next file would corrupt the output.
		headerWritten = true
		if err != nil {
			log.Printf("  warning: error reading %s: %v", tmpPath, err)
		} else {
			totalRows += rows
			fmt.Printf("  %s — %d rows\n", filepath.Base(tmpPath), rows)
		}
	}
	return totalRows, nil
}

// copyCSV copies src into dst. If keepHeader is true the first line is copied;
// otherwise it is skipped. Returns the number of data rows copied.
func copyCSV(src, dst *os.File, keepHeader bool) (int, error) {
	buf := make([]byte, 4*1024*1024) // 4 MB buffer
	rows := 0
	firstLine := true
	remainder := []byte{}

	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			chunk := append(remainder, buf[:n]...)
			start := 0
			for i, b := range chunk {
				if b == '\n' {
					line := chunk[start : i+1]
					start = i + 1
					if firstLine {
						firstLine = false
						if keepHeader {
							dst.Write(line)
							// header line is not a data row — do not increment rows
						}
						continue
					}
					dst.Write(line)
					rows++
				}
			}
			remainder = append([]byte{}, chunk[start:]...)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return rows, readErr
		}
	}

	// Flush any remaining bytes (line without trailing newline)
	if len(remainder) > 0 {
		if !firstLine { // not a header-only file
			dst.Write(remainder)
			dst.Write([]byte("\n"))
			rows++
		}
	}
	return rows, nil
}

func cleanupTempFiles(files []string) {
	for _, f := range files {
		if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
			log.Printf("warning: could not remove temp file %s: %v", f, err)
		}
	}
}