// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !windows

package sysutil

import (
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestRefreshSignaledChan(t *testing.T) {
	ch := RefreshSignaledChan()

	if err := unix.Kill(unix.Getpid(), refreshSignal); err != nil {
		t.Error(err)
		return
	}

	select {
	case sig := <-ch:
		if refreshSignal != sig {
			t.Fatalf("expected signal %s, but got %s", refreshSignal, sig)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out while waiting for refresh signal")
	}
}
