package gopter

import "time"

type testStatus int

const (
	// TestPassed indicates that the property check has passed.
	TestPassed testStatus = iota
	// TestProved indicates that the property has been proved.
	TestProved
	// TestFailed indicates that the property check has failed.
	TestFailed
	// TestExhausted indicates that the property check has exhausted, i.e. the generators have
	// generated too many empty results.
	TestExhausted
	// TestError indicates that the property check has finished with an error.
	TestError
)

func (s testStatus) String() string {
	switch s {
	case TestPassed:
		return "PASSED"
	case TestProved:
		return "PROVED"
	case TestFailed:
		return "FAILED"
	case TestExhausted:
		return "EXHAUSTED"
	case TestError:
		return "ERROR"
	}
	return ""
}

// TestResult contains the result of a property property check.
type TestResult struct {
	Status     testStatus
	Succeeded  int
	Discarded  int
	Labels     []string
	Error      error
	ErrorStack []byte
	Args       PropArgs
	Time       time.Duration
}

// Passed checks if the check has passed
func (r *TestResult) Passed() bool {
	return r.Status == TestPassed || r.Status == TestProved
}
