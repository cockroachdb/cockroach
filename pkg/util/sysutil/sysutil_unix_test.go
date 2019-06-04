// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
