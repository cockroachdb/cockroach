package roachtestutil

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestGoAfterFires(t *testing.T) {
	defer leaktest.AfterTest(t)()
	done := make(chan struct{})
	cleanup := GoAfter(context.Background(), time.Millisecond, func() { close(done) })
	<-done // This test will timeout if done is never closed
	cleanup()
}

func TestGoAfterCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cleanup := GoAfter(context.Background(), time.Minute, func() {
		// This should never run because cleanup is called
		t.Fail()
	})
	cleanup()
}
