// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package repro

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBloke(t *testing.T) {
	log := testLogger{}
	bloke := NewBloke[Bus](&log)
	go func() {
		time.Sleep(100 * time.Millisecond)
		bloke.Step(0, "first")
	}()
	bloke.Step(1, "second")
}

func TestBlokeBus(t *testing.T) {
	log := testLogger{}
	type bus struct {
		id   int
		info string
	}
	bloke := NewBloke[bus](&log)

	go func() {
		time.Sleep(100 * time.Millisecond)
		bloke.StepEx(0, func(bus *bus) bool {
			bus.id = 5
			return true
		}, "first")

		bloke.StepIf(3, func(b *bus) bool {
			require.Equal(t, 5, b.id)
			require.Equal(t, "info", b.info)
			return true
		}, "fourth")
	}()

	bloke.StepIf(1, func(bus *bus) bool {
		return bus.id == 5
	}, "second")

	bloke.StepEx(2, func(b *bus) bool {
		require.Equal(t, 5, b.id)
		b.info = "info"
		return true
	}, "third")

	bloke.Step(4, "done")

	want := "REPRO[0]: first\n" +
		"REPRO[1]: second\n" +
		"REPRO[2]: third\n" +
		"REPRO[3]: fourth\n" +
		"REPRO[4]: done\n"
	require.Equal(t, want, log.String())
}

type testLogger struct {
	buf []byte
}

func (l *testLogger) Warningf(ctx context.Context, format string, args ...interface{}) {
	l.buf = fmt.Appendf(l.buf, format, args...)
	l.buf = append(l.buf, '\n')
}

func (l *testLogger) String() string {
	return string(l.buf)
}
