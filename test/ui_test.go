package test

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/civil"
	"github.com/wakatara/harsh/internal/storage"
	"github.com/wakatara/harsh/internal/ui"
)

func TestColorManager(t *testing.T) {
	// Test with colors enabled
	cm := ui.NewColorManager(false)
	if cm.IsDisabled() {
		t.Error("Color manager should not be disabled")
	}

	// Test with colors disabled
	cmNoColor := ui.NewColorManager(true)
	if !cmNoColor.IsDisabled() {
		t.Error("Color manager should be disabled")
	}

	// Test SetNoColor
	cm.SetNoColor(true)
	if !cm.IsDisabled() {
		t.Error("Color manager should be disabled after SetNoColor(true)")
	}

	cm.SetNoColor(false)
	if cm.IsDisabled() {
		t.Error("Color manager should not be disabled after SetNoColor(false)")
	}
}

func TestGetTodos(t *testing.T) {
	habits := []*storage.Habit{
		{Name: "Test1", Target: 1, Interval: 1, FirstRecord: civil.Date{Year: 2025, Month: 1, Day: 1}},
		{Name: "Test2", Target: 1, Interval: 1, FirstRecord: civil.Date{Year: 2025, Month: 1, Day: 1}},
		{Name: "Test3", Target: 1, Interval: 1, FirstRecord: civil.Date{Year: 2025, Month: 1, Day: 1}},
	}

	entries := &storage.Entries{
		storage.DailyHabit{Day: civil.Date{Year: 2025, Month: 1, Day: 15}, Habit: "Test1"}: {Result: "y"},
		storage.DailyHabit{Day: civil.Date{Year: 2025, Month: 1, Day: 15}, Habit: "Test2"}: {Result: "y"},
		// Test3 is missing (should be in todos)
	}

	todos := ui.GetTodos(habits, entries, civil.Date{Year: 2025, Month: 1, Day: 15}, 1)

	// Should have entries for the day
	if len(todos) == 0 {
		t.Error("Should have todo entries")
	}

	// Should contain Test3
	found := false
	for _, todoList := range todos {
		for _, todo := range todoList {
			if todo == "Test3" {
				found = true
				break
			}
		}
	}
	if !found {
		t.Error("Test3 should be in todos")
	}

	// Test with onboarding (0 days back)
	onboardTodos := ui.GetTodos(habits, entries, civil.Date{Year: 2025, Month: 1, Day: 15}, 0)
	if len(onboardTodos) == 0 {
		t.Error("Should have onboarding todos")
	}
}

func TestUIBuildStats(t *testing.T) {
	habit := &storage.Habit{
		Name:        "Test",
		Target:      1,
		Interval:    1,
		FirstRecord: civil.Date{Year: 2025, Month: 1, Day: 10},
	}

	entries := &storage.Entries{
		storage.DailyHabit{Day: civil.Date{Year: 2025, Month: 1, Day: 10}, Habit: "Test"}: {Result: "y", Amount: 5.0},
		storage.DailyHabit{Day: civil.Date{Year: 2025, Month: 1, Day: 11}, Habit: "Test"}: {Result: "y", Amount: 3.0},
		storage.DailyHabit{Day: civil.Date{Year: 2025, Month: 1, Day: 12}, Habit: "Test"}: {Result: "n", Amount: 0.0},
		storage.DailyHabit{Day: civil.Date{Year: 2025, Month: 1, Day: 13}, Habit: "Test"}: {Result: "s", Amount: 0.0},
		storage.DailyHabit{Day: civil.Date{Year: 2025, Month: 1, Day: 14}, Habit: "Test"}: {Result: "y", Amount: 2.0},
	}

	stats := ui.BuildStats(habit, entries)

	// Check the stats
	if stats.Streaks != 3 {
		t.Errorf("Expected 3 streaks, got %d", stats.Streaks)
	}

	if stats.Breaks != 1 {
		t.Errorf("Expected 1 break, got %d", stats.Breaks)
	}

	if stats.Skips != 1 {
		t.Errorf("Expected 1 skip, got %d", stats.Skips)
	}

	if stats.Total != 10.0 {
		t.Errorf("Expected total 10.0, got %f", stats.Total)
	}

	if stats.DaysTracked <= 0 {
		t.Errorf("Expected positive days tracked, got %d", stats.DaysTracked)
	}
}

func TestDisplayShowHabitLog(t *testing.T) {
	// Create test data
	habits := []*storage.Habit{
		{Name: "Test1", Heading: "Work", Target: 1, Interval: 1, FirstRecord: civil.Date{Year: 2025, Month: 1, Day: 1}},
		{Name: "Test2", Heading: "Health", Target: 1, Interval: 1, FirstRecord: civil.Date{Year: 2025, Month: 1, Day: 1}},
	}

	entries := &storage.Entries{
		storage.DailyHabit{Day: civil.Date{Year: 2025, Month: 1, Day: 15}, Habit: "Test1"}: {Result: "y"},
		storage.DailyHabit{Day: civil.Date{Year: 2025, Month: 1, Day: 15}, Habit: "Test2"}: {Result: "n"},
	}

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	display := ui.NewDisplay(true) // no color for testing
	display.ShowHabitLog(habits, entries, 10, 20, "")

	// Restore stdout
	w.Close()
	os.Stdout = old

	// Read captured output
	buf := new(bytes.Buffer)
	buf.ReadFrom(r)
	output := buf.String()

	// Should contain habit names
	if !strings.Contains(output, "Test1") {
		t.Error("Output should contain Test1")
	}

	// Should contain headings
	if !strings.Contains(output, "Work") {
		t.Error("Output should contain Work heading")
	}

	// Should contain score information
	if !strings.Contains(output, "Score") {
		t.Error("Output should contain score information")
	}
}

func TestDisplayShowHabitStats(t *testing.T) {
	// Create test data
	habits := []*storage.Habit{
		{Name: "Test1", Heading: "Work", Target: 1, Interval: 1, FirstRecord: civil.Date{Year: 2025, Month: 1, Day: 1}},
	}

	entries := &storage.Entries{
		storage.DailyHabit{Day: civil.Date{Year: 2025, Month: 1, Day: 15}, Habit: "Test1"}: {Result: "y", Amount: 5.0},
	}

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	display := ui.NewDisplay(true) // no color for testing
	display.ShowHabitStats(habits, entries, 20)

	// Restore stdout
	w.Close()
	os.Stdout = old

	// Read captured output
	buf := new(bytes.Buffer)
	buf.ReadFrom(r)
	output := buf.String()

	// Should contain habit name
	if !strings.Contains(output, "Test1") {
		t.Error("Output should contain Test1")
	}

	// Should contain stats terms
	if !strings.Contains(output, "Streaks") {
		t.Error("Output should contain Streaks")
	}

	if !strings.Contains(output, "Breaks") {
		t.Error("Output should contain Breaks")
	}

	if !strings.Contains(output, "Skips") {
		t.Error("Output should contain Skips")
	}

	if !strings.Contains(output, "Total") {
		t.Error("Output should contain Total")
	}
}

func TestDisplayShowTodos(t *testing.T) {
	// Create test data
	habits := []*storage.Habit{
		{Name: "Test1", Heading: "Work", Target: 1, Interval: 1, FirstRecord: civil.Date{Year: 2025, Month: 1, Day: 1}},
	}

	entries := &storage.Entries{}

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	display := ui.NewDisplay(true) // no color for testing
	display.ShowTodos(habits, entries, 20)

	// Restore stdout
	w.Close()
	os.Stdout = old

	// Read captured output
	buf := new(bytes.Buffer)
	buf.ReadFrom(r)
	output := buf.String()

	// Should contain habit name or indicate all complete
	if !strings.Contains(output, "Test1") && !strings.Contains(output, "All todos logged") {
		t.Error("Output should contain Test1 or indicate completion")
	}
}

func TestInputOnboard(t *testing.T) {
	// Create a mock input
	input := ui.NewInput(true)

	// For testing, we can't easily mock stdin, so we'll just verify the function exists
	// In a real test, you'd use a mocked reader
	if input == nil {
		t.Error("Input should not be nil")
	}

	// The Onboard function would require mocking stdin for proper testing
	// This is a structural test to ensure the function exists
}

// TestMockRepository tests the repository interface compliance
func TestMockRepository(t *testing.T) {
	// Create a mock repository for testing
	mockRepo := &MockRepository{
		habits: []*storage.Habit{
			{Name: "Test", Target: 1, Interval: 1},
		},
		log: &storage.Log { Entries: storage.Entries{}, Header: storage.DefaultHeader },
	}

	// Test interface compliance
	var _ storage.Repository = mockRepo

	// Test LoadHabits
	habits, maxLength, err := mockRepo.LoadHabits()
	if err != nil {
		t.Fatal(err)
	}
	if len(habits) != 1 {
		t.Errorf("Expected 1 habit, got %d", len(habits))
	}
	if maxLength == 0 {
		t.Error("Max length should be positive")
	}

	// Test LoadEntries
	log, err := mockRepo.LoadEntries()
	if err != nil {
		t.Fatal(err)
	}
	if log == nil {
		t.Error("Entries should not be nil")
	}

	// Test WriteEntry
	testDate := civil.Date{Year: 2025, Month: 1, Day: 15}
	err = mockRepo.WriteEntry(testDate, "Test", "y", "comment", "1.0", log.Header)
	if err != nil {
		t.Fatal(err)
	}

	// Verify entry was written
	log, _ = mockRepo.LoadEntries()
	entry := log.Entries[storage.DailyHabit{Day: testDate, Habit: "Test"}]
	if entry.Result != "y" {
		t.Errorf("Entry not written correctly: got %s, want y", entry.Result)
	}
}

// MockRepository for testing
type MockRepository struct {
	habits  []*storage.Habit
	log *storage.Log
}

func (m *MockRepository) LoadHabits() ([]*storage.Habit, int, error) {
	maxLength := 0
	for _, habit := range m.habits {
		if len(habit.Name) > maxLength {
			maxLength = len(habit.Name)
		}
	}
	return m.habits, maxLength + 10, nil
}

func (m *MockRepository) LoadEntries() (*storage.Log, error) {
	return m.log, nil
}

func (m *MockRepository) WriteEntry(d civil.Date, habit string, result string, comment string, amount string, header storage.Header) error {
	famount := 0.0
	if amount != "" {
		// In a real implementation, we'd parse the amount
		famount = 1.0
	}
	m.log.Entries[storage.DailyHabit{Day: d, Habit: habit}] = storage.Outcome{
		Result:  result,
		Comment: comment,
		Amount:  famount,
	}
	return nil
}

func (m *MockRepository) GetConfigDir() string {
	return "/tmp/test"
}

func (m *MockRepository) InitializeConfig() error {
	return nil
}

func TestHabitStatsStruct(t *testing.T) {
	// Test HabitStats struct
	stats := ui.HabitStats{
		DaysTracked: 30,
		Total:       150.5,
		Streaks:     25,
		Breaks:      3,
		Skips:       2,
	}

	if stats.DaysTracked != 30 {
		t.Errorf("Expected 30 days tracked, got %d", stats.DaysTracked)
	}

	if stats.Total != 150.5 {
		t.Errorf("Expected total 150.5, got %f", stats.Total)
	}

	if stats.Streaks != 25 {
		t.Errorf("Expected 25 streaks, got %d", stats.Streaks)
	}

	if stats.Breaks != 3 {
		t.Errorf("Expected 3 breaks, got %d", stats.Breaks)
	}

	if stats.Skips != 2 {
		t.Errorf("Expected 2 skips, got %d", stats.Skips)
	}
}
