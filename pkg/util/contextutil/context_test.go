package contextutil

import (
	"context"
	"testing"

	"time"

	"github.com/pkg/errors"
)

func TestCancelWithReason(t *testing.T) {
	ctx := context.Background()

	var cancel context.CancelFunc
	ctx, cancel = WithCancel(ctx)

	go func() {
		CancelWithReason(ctx, cancel, errors.New("hodor"))
	}()

loop:
	for true {
		select {
		case <-ctx.Done():
			break loop
		}
		time.Sleep(time.Duration(100 * time.Millisecond))
	}

	expected := "context canceled: hodor"
	found := ctx.Err().Error()
	if found != expected {
		t.Fatalf("found %s, expected %s", found, expected)
	}
}
