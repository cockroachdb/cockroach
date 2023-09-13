package microbisect

import (
	"testing"
	"time"
)

func BenchmarkTest(t *testing.B) {
	time.Sleep(5 * time.Millisecond)
}
