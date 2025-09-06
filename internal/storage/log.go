package storage
import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"cloud.google.com/go/civil"
)

// Outcome is the explicit recorded result of a habit
// on a day (y, n, or s) and an optional amount and comment
type Outcome struct {
	Result  string
	Amount  float64
	Comment string
}

// DailyHabit combines Day and Habit with an Outcome to yield Entries
type DailyHabit struct {
	Day   civil.Date
	Habit string
}

// Entries maps DailyHabit{ISO date + habit}: Outcome and log format
type Entries map[DailyHabit]Outcome
type Header map[string]int

const (
	HeaderDate = "Date"
	HeaderAmount = "Amount"
	HeaderComment = "Comment"
	HeaderHabit = "Habit"
	HeaderStatus = "Status"
)

var DefaultHeader = Header {
	HeaderDate: 0,
	HeaderHabit: 1,
	HeaderStatus: 2,
	HeaderComment: 3,
	HeaderAmount: 4,
}

type Log struct {
	Entries Entries
	Header Header
}

// LoadLog reads entries from log file
func LoadLog(configDir string) *Log {
	logPath := filepath.Join(configDir, "/log")
	file, err := os.Open(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Check for common cloud storage scenarios
			icloudPath := filepath.Join(configDir, ".log.icloud")
			if _, err := os.Stat(icloudPath); err == nil {
				fmt.Println("Error: Your log file is currently syncing with iCloud.")
				fmt.Println("The file appears as '.log.icloud' while syncing.")
				fmt.Println("Please wait for sync to complete, or disable iCloud for the harsh folder.")
				os.Exit(1)
			}

			// Check if config directory exists but log file doesn't
			if _, err := os.Stat(configDir); err == nil {
				fmt.Printf("Error: Log file not found at %s\n", logPath)
				fmt.Println("This might be your first time using harsh.")
				fmt.Println("Run 'harsh' without arguments to initialize your configuration.")
				os.Exit(1)
			}

			// Config directory doesn't exist
			fmt.Printf("Error: Configuration directory not found at %s\n", configDir)
			fmt.Println("Run 'harsh' without arguments to initialize your configuration.")
			os.Exit(1)
		}

		// For permission errors or other issues, provide context
		if os.IsPermission(err) {
			fmt.Printf("Error: Permission denied accessing log file at %s\n", logPath)
			fmt.Println("Check file permissions or try running with appropriate privileges.")
			os.Exit(1)
		}

		// For other errors, use the original behavior but with more context
		fmt.Printf("Error opening log file at %s: %v\n", logPath, err)
		os.Exit(1)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	entries := Entries{}
	lineCount := 0
	scanner.Scan()
	header, err := ParseHeader(scanner.Text())
	if err != nil {
		header = DefaultHeader
		lineCount++
		parseLogLine(scanner.Text(), lineCount, header, entries)
	}
	for scanner.Scan() {
		lineCount++
		parseLogLine(scanner.Text(), lineCount, header, entries)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return &Log {
		Entries: entries,
		Header: header,
	}
}

func ParseHeader(line string) (Header, error) {
	result := strings.Split(line, " : ")
	out := make(map[string]int, len(result))
	for i, word := range result {
		switch word {
		case HeaderDate,HeaderHabit,HeaderStatus,HeaderComment,HeaderAmount:
			out[word] = i
		default:
			return nil, errors.New("not a header")
		}
	}
	return out, nil
}

func parseLogLine(line string, lineCount int, header map[string]int, entries Entries) {
	if len(line) > 0 {
		if line[0] != '#' {
			// Discards comments from read record read as result[header[HeaderComment]]
			result := strings.Split(line, " : ")

			// Warn for entries that have less than header's count
			if len(result) != len(header) {
				fmt.Printf("Warning: expected (%d) fields, found (%d) at line %d\n", len(header), len(result), lineCount)
			}

			var cd civil.Date
			if i, ok := header[HeaderDate]; ok && i < len(result) {
				var err error
				cd, err = civil.ParseDate(result[i])
				if err != nil {
					fmt.Printf("Warning: Skipping log entry with invalid date at line %d: %s\n", lineCount, result[i])
					return
				}
			}

			if i, ok := header[HeaderHabit]; !ok || i >= len(result) || strings.TrimSpace(result[i]) == "" {
				// Validate habit name is not empty
				fmt.Printf("Warning: Skipping log entry with empty habit name at line %d\n", lineCount)
				return
			}

			// Validate result is y, n, or s
			if i, ok := header[HeaderStatus]; ok && i < len(result) {
				result[i] = strings.TrimSpace(result[i])
				if result[i] != "y" && result[i] != "n" && result[i] != "s" {
					fmt.Printf("Warning: Skipping log entry with invalid result '%s' at line %d (expected y/n/s)\n", result[i], lineCount)
					return
				}
			}

			var amount float64
			if  i, ok := header[HeaderAmount]; ok && i < len(result) && result[i] != "" {
				var err error
				amount, err = strconv.ParseFloat(result[i], 64)
				if err != nil {
					fmt.Printf("Warning: Invalid amount '%s' at line %d, using %f\n", result[i], lineCount, amount)
				}
			}

			var comment string
			if i, ok := header[HeaderComment]; ok && i < len(result) {
				comment = result[i]
			}
			entries[DailyHabit{Day: cd, Habit: result[header[HeaderHabit]]}] = Outcome{Result: result[header[HeaderStatus]], Comment: comment, Amount: amount}
		}
	}
}

// WriteHabitLog writes the log entry for a habit to file
func WriteHabitLog(configDir string, d civil.Date, habit string, result string, comment string, amount string, header Header) error {
	fileName := filepath.Join(configDir, "/log")
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		// Provide more specific error messages based on the type of error
		if os.IsNotExist(err) {
			return fmt.Errorf("configuration directory does not exist: %s", configDir)
		}
		if os.IsPermission(err) {
			return fmt.Errorf("permission denied writing to log file: %s (check file permissions)", fileName)
		}
		// Check for disk space issues (this is a common cause of write failures)
		return fmt.Errorf("cannot open log file %s: %w (this might be due to insufficient disk space or file system issues)", fileName, err)
	}
	defer f.Close()
	fields := make([]string, len(header))
	for header, i := range header {
		var field string
		switch header {
		case HeaderAmount:
			field = amount
		case HeaderComment:
			field = comment
		case HeaderDate:
			field = d.String()
		case HeaderHabit:
			field = habit
		case HeaderStatus:
			field = result
		}
		fields[i] = field
	}
	logEntry := strings.Join(fields, " : ") + "\n"
	if _, err := f.Write([]byte(logEntry)); err != nil {
		f.Close() // ignore error; Write error takes precedence
		// Check for common write failure causes
		if strings.Contains(err.Error(), "no space left") || strings.Contains(err.Error(), "disk full") {
			return fmt.Errorf("failed to write log entry: disk full or insufficient space")
		}
		return fmt.Errorf("failed to write log entry to %s: %w", fileName, err)
	}
	if err := f.Close(); err != nil {
		// Convert this from log.Fatal to a proper error return
		return fmt.Errorf("failed to close log file %s: %w", fileName, err)
	}
	return nil
}

// FirstRecords sets the FirstRecord field for habits based on their earliest entries
func (e *Entries) FirstRecords(from civil.Date, to civil.Date, habits []*Habit) {
	for dt := to; !dt.Before(from); dt = dt.AddDays(-1) {
		for _, habit := range habits {
			if _, ok := (*e)[DailyHabit{Day: dt, Habit: habit.Name}]; ok {
				habit.FirstRecord = dt
			}
		}
	}
}
