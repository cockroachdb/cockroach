// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"net"
	"os"
	"path"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestInitHandshake(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	timeout := 11 * time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	tempDir, del := testutils.TempDir(t)
	defer del()

	cfg1 := &base.Config{}
	cfg1.InitDefaults()
	cfg1.SSLCertsDir = path.Join(tempDir, "temp1")
	require.NoError(t, os.Mkdir(cfg1.SSLCertsDir, 0755))

	cfg2 := &base.Config{}
	cfg2.InitDefaults()
	cfg2.SSLCertsDir = path.Join(tempDir, "temp2")
	require.NoError(t, os.Mkdir(cfg2.SSLCertsDir, 0755))

	cfg3 := &base.Config{}
	cfg3.InitDefaults()
	cfg3.SSLCertsDir = path.Join(tempDir, "temp3")
	require.NoError(t, os.Mkdir(cfg3.SSLCertsDir, 0755))

	errReturned := make(chan error, 1)
	// Do a three-node handshake, and ensure no error is returned. The errors
	// returned should be nil, and one of the temp SSL certs directories should
	// not be empty.
	var addr1, addr2, addr3 string

	listener1, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		_ = listener1.Close()
	}()
	addr1 = listener1.Addr().String()

	listener2, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		_ = listener2.Close()
	}()
	addr2 = listener2.Addr().String()

	listener3, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		_ = listener3.Close()
	}()
	addr3 = listener3.Addr().String()

	go func() {
		errReturned <- InitHandshake(ctx, cfg1, "foobar", 2, []string{addr2, addr3}, cfg1.SSLCertsDir, listener1)
	}()
	go func() {
		errReturned <- InitHandshake(ctx, cfg2, "foobar", 2, []string{addr1, addr3}, cfg1.SSLCertsDir, listener2)
	}()
	go func() {
		errReturned <- InitHandshake(ctx, cfg3, "foobar", 2, []string{addr1, addr2}, cfg1.SSLCertsDir, listener3)
	}()

	count := 0
	// All of the errors returned to errReturned should be nil.
	var combined error
	for count < 3 {
		select {
		case err := <-errReturned:
			combined = errors.CombineErrors(combined, err)
			count++
		case <-ctx.Done():
			t.Fatal("test timeout exceeded")
		}
	}
	require.NoError(t, combined)
	dirs := []string{cfg1.SSLCertsDir, cfg2.SSLCertsDir, cfg3.SSLCertsDir}
	// All of the directories should be non-empty, indicating a successful
	// provisioning of the init bundle.
	for _, dir := range dirs {
		f, err := os.Open(dir)
		require.NoError(t, err)
		_, err = f.Readdirnames(1)
		// An error is returned if the directory is empty.
		require.NoError(t, err)
	}
}

func TestInitHandshakeWrongToken(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// The test deadline needs to be greater than this for this test to pass,
	// as one of the nodes will have to wait for this context to time out.
	timeout := 30 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	tempDir, del := testutils.TempDir(t)
	defer del()

	cfg1 := &base.Config{}
	cfg1.InitDefaults()
	cfg1.SSLCertsDir = path.Join(tempDir, "temp1")
	require.NoError(t, os.Mkdir(cfg1.SSLCertsDir, 0755))

	cfg2 := &base.Config{}
	cfg2.InitDefaults()
	cfg2.SSLCertsDir = path.Join(tempDir, "temp2")
	require.NoError(t, os.Mkdir(cfg2.SSLCertsDir, 0755))

	cfg3 := &base.Config{}
	cfg3.InitDefaults()
	cfg3.SSLCertsDir = path.Join(tempDir, "temp3")
	require.NoError(t, os.Mkdir(cfg3.SSLCertsDir, 0755))

	errReturned := make(chan error, 1)
	// Do a three-node handshake, with one node having the wrong token. At least
	// one of the three errors returned should be non-nil.
	var addr1, addr2, addr3 string

	listener1, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		_ = listener1.Close()
	}()
	addr1 = listener1.Addr().String()

	listener2, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		_ = listener2.Close()
	}()
	addr2 = listener2.Addr().String()

	listener3, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		_ = listener3.Close()
	}()
	addr3 = listener3.Addr().String()

	go func() {
		errReturned <- InitHandshake(ctx, cfg1, "foobar", 2, []string{addr2, addr3}, cfg1.SSLCertsDir, listener1)
	}()
	go func() {
		errReturned <- InitHandshake(ctx, cfg2, "foobarbaz", 2, []string{addr1, addr3}, cfg1.SSLCertsDir, listener2)
	}()
	go func() {
		errReturned <- InitHandshake(ctx, cfg3, "foobar", 2, []string{addr1, addr2}, cfg1.SSLCertsDir, listener3)
	}()

	count := 0
	// All of the errors returned to errReturned should not be nil.
	for count < 3 {
		select {
		case err := <-errReturned:
			require.Error(t, err)
			count++
		case <-time.After(80 * time.Second):
			// The timeout here should be greater than the timeout at the beginning
			// of the test.
			t.Fatal("test timed out")
		}
	}
	dirs := []string{cfg1.SSLCertsDir, cfg2.SSLCertsDir, cfg3.SSLCertsDir}
	found := false
	// All of the cert dirs should be empty, indicating an unsuccessful
	// provisioning of the init bundle.
	for _, dir := range dirs {
		f, err := os.Open(dir)
		require.NoError(t, err)
		_, err = f.Readdirnames(1)
		if err == nil {
			found = true
			break
		}
	}
	require.False(t, found)
}
