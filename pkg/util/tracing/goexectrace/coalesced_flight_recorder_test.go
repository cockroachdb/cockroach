// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goexectrace

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestCoalescedFlightRecorder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var nameIdx int
	files := map[string]*bytes.Buffer{}
	opts := CoalescedFlightRecorderOptions{
		UniqueFileName: func() string {
			nameIdx++
			return fmt.Sprintf("file_%d.bin", nameIdx)
		},
		CreateFile: func(s string) (io.Writer, error) {
			files[s] = &bytes.Buffer{}
			return files[s], nil
		},
		IdleDuration: time.Nanosecond,
		Warningf:     t.Logf,
	}
	cfr := NewCoalescedFlightRecorder(opts)

	hdl := cfr.New()
	defer hdl.Release()

	name, futErr := hdl.Dump()
	t.Log(name)
	select {
	case <-futErr.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out")
	}
	require.NoError(t, futErr.Get())
	require.Len(t, files, 1)
	require.NotZero(t, files["file_1.bin"].Bytes())
}
