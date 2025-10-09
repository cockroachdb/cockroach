// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build go1.25

package storage

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

type logSnooper struct {
	mu struct {
		syncutil.Mutex
		buf bytes.Buffer
	}
	pebble.LoggerAndTracer
}

func (l *logSnooper) Infof(format string, args ...interface{}) {
	l.LoggerAndTracer.Infof(format, args...)
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(&l.mu.buf, format, args...)
}

func makeInMemPebbleLogger() *logSnooper {
	return &logSnooper{
		LoggerAndTracer: &pebbleLogger{ctx: context.Background(), depth: 1},
	}
}

func TestPebbleIteratorTracking(t *testing.T) {
	defer leaktest.AfterTest(t)()

	synctest.Test(t, func(t *testing.T) {
		// Capture long-lived iterator reports by swapping out the logger.
		snooper := makeInMemPebbleLogger()
		opt := func(cfg *engineConfig) error {
			cfg.opts.LoggerAndTracer = snooper
			return nil
		}
		p := createTestPebbleEngine(opt).(*Pebble)
		defer p.Close()
		// Insert some data into the DB.
		ek := engineKey("foo", 0)
		require.NoError(t, p.PutEngineKey(ek, nil))
		// Construct a pebbleIterator over the DB.
		iterOpts := IterOptions{
			LowerBound: []byte("a"),
			UpperBound: []byte("z"),
		}
		iter, err := newPebbleIterator(context.Background(), p.db, iterOpts, StandardDurability, p)
		require.NoError(t, err)
		defer iter.Close()
		time.Sleep(15 * time.Minute)
		logs := func() string {
			snooper.mu.Lock()
			defer snooper.mu.Unlock()
			return snooper.mu.buf.String()
		}()

		// Pebble should have reported the long-lived iterator. Sample logs:
		//
		//   pebble_iterator_tracking_test.go:77: Long-lived iterators detected:
		//     started 5m0s ago:
		//      github.com/cockroachdb/pebble.(*DB).newIter
		//       /Users/radu/go/pkg/mod/github.com/cockroachdb/pebble@v0.0.0-20251007185819-79e6b4a13387/db.go:1119
		//      github.com/cockroachdb/pebble.(*DB).NewIterWithContext
		//       /Users/radu/go/pkg/mod/github.com/cockroachdb/pebble@v0.0.0-20251007185819-79e6b4a13387/db.go:1421
		//      github.com/cockroachdb/cockroach/pkg/storage.newPebbleIterator
		//       /Users/radu/roach/pkg/storage/pebble_iterator.go:100
		//      github.com/cockroachdb/cockroach/pkg/storage.TestPebbleIteratorTracking.func1
		//       /Users/radu/roach/pkg/storage/pebble_iterator_tracking_test.go:65
		//      testing.tRunner
		//       /Users/radu/go/go1.25.1/src/testing/testing.go:1934
		//      runtime.goexit
		//       /Users/radu/go/go1.25.1/src/runtime/asm_arm64.s:1268
		require.Contains(t, logs, "Long-lived iterators detected")
		require.Contains(t, logs, "TestPebbleIteratorTracking")
	})
}
