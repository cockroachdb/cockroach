package gopter

// Reporter is a simple interface to report/format the results of a property check.
type Reporter interface {
	// ReportTestResult reports a single property result
	ReportTestResult(propName string, result *TestResult)
}
