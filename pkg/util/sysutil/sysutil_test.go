// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sysutil

import (
	"net"
	"os/exec"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestExitStatus(t *testing.T) {
	cmd := exec.Command("sh", "-c", "exit 42")
	err := cmd.Run()
	if err == nil {
		t.Fatalf("%s did not return error", cmd.Args)
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("%s returned error of type %T, but expected *exec.ExitError", cmd.Args, err)
	}
	if status := ExitStatus(exitErr); status != 42 {
		t.Fatalf("expected exit status 42, but got %d", status)
	}
}

func TestIsAddrInUse(t *testing.T) {
	ln, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	_, err = net.Listen("tcp", ln.Addr().String())
	require.True(t, IsAddrInUse(err))
}
