package test

// Monitor is an interface for monitoring cockroach processes during a test.
type Monitor interface {
	ExpectDeath()
	ExpectDeaths(count int32)
	ResetDeaths()
}
