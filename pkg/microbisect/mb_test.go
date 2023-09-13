package microbisect

import (
	"testing"
	"time"
)

func BenchmarkTest(t *testing.B) {
	time.Sleep(25 * time.Millisecond)
}
