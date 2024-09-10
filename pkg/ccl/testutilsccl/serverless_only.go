package testutilsccl

// ServerlessOnly is called in tests to mark them as testing functionality that
// is Serverless specific. This is changed from a no-op to a test skip once
func ServerlessOnly() {
	// Uncomment in release branches that no longer support serverless.
	// skip.WithIssue(t, 130441)
}
