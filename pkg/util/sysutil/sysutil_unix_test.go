// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !windows

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
