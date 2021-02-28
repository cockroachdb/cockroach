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
	"os"
	"path"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/stretchr/testify/require"
)

func TestInitHandshake(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderShort(t)

	timeout := 11 * time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	tempDir, del := testutils.TempDir(t)
	defer del()

	ctx1 := logtags.AddTag(ctx, "n", 1)
	cfg1 := &base.Config{}
	cfg1.InitDefaults()
	cfg1.SSLCertsDir = path.Join(tempDir, "temp1")
	cfg1.Addr = "127.0.0.1:0"
	require.NoError(t, os.Mkdir(cfg1.SSLCertsDir, 0755))
	require.NoError(t, cfg1.ValidateAddrs(ctx1))

	ctx2 := logtags.AddTag(ctx, "n", 2)
	cfg2 := &base.Config{}
	cfg2.InitDefaults()
	cfg2.SSLCertsDir = path.Join(tempDir, "temp2")
	cfg2.Addr = "127.0.0.1:0"
	require.NoError(t, os.Mkdir(cfg2.SSLCertsDir, 0755))
	require.NoError(t, cfg2.ValidateAddrs(ctx2))

	ctx3 := logtags.AddTag(ctx, "n", 3)
	cfg3 := &base.Config{}
	cfg3.InitDefaults()
	cfg3.SSLCertsDir = path.Join(tempDir, "temp3")
	cfg3.Addr = "127.0.0.1:0"
	require.NoError(t, os.Mkdir(cfg3.SSLCertsDir, 0755))
	require.NoError(t, cfg3.ValidateAddrs(ctx3))

	errReturned := make(chan error, 1)
	// Do a three-node handshake, and ensure no error is returned. The errors
	// returned should be nil, and one of the temp SSL certs directories should
	// not be empty.
	var addr1, addr2, addr3 string

	listener1, err := ListenAndUpdateAddrs(ctx1, &cfg1.Addr, &cfg1.AdvertiseAddr, "rpc")
	require.NoError(t, err)
	defer func() {
		_ = listener1.Close()
	}()
	addr1 = listener1.Addr().String()

	listener2, err := ListenAndUpdateAddrs(ctx2, &cfg2.Addr, &cfg2.AdvertiseAddr, "rpc")
	require.NoError(t, err)
	defer func() {
		_ = listener2.Close()
	}()
	addr2 = listener2.Addr().String()

	listener3, err := ListenAndUpdateAddrs(ctx3, &cfg3.Addr, &cfg3.AdvertiseAddr, "rpc")
	require.NoError(t, err)
	defer func() {
		_ = listener3.Close()
	}()
	addr3 = listener3.Addr().String()

	reporter := func(prefix string) func(string, ...interface{}) {
		return func(format string, args ...interface{}) {
			t.Logf(prefix+": "+format, args...)
		}
	}

	go func() {
		errReturned <- InitHandshake(ctx1, reporter("n1"), cfg1, "foobar", 3, []string{addr2, addr3}, cfg1.SSLCertsDir, listener1)
	}()
	go func() {
		errReturned <- InitHandshake(ctx2, reporter("n2"), cfg2, "foobar", 3, []string{addr1, addr3}, cfg2.SSLCertsDir, listener2)
	}()
	go func() {
		errReturned <- InitHandshake(ctx3, reporter("n3"), cfg3, "foobar", 3, []string{addr1, addr2}, cfg3.SSLCertsDir, listener3)
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

	skip.UnderShort(t)

	// The test deadline needs to be greater than this for this test to pass,
	// as one of the nodes will have to wait for this context to time out.
	timeout := 20 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	tempDir, del := testutils.TempDir(t)
	defer del()

	ctx1 := logtags.AddTag(ctx, "n", 1)
	cfg1 := &base.Config{}
	cfg1.InitDefaults()
	cfg1.SSLCertsDir = path.Join(tempDir, "temp1")
	cfg1.Addr = "127.0.0.1:0"
	require.NoError(t, os.Mkdir(cfg1.SSLCertsDir, 0755))
	require.NoError(t, cfg1.ValidateAddrs(ctx1))

	ctx2 := logtags.AddTag(ctx, "n", 2)
	cfg2 := &base.Config{}
	cfg2.InitDefaults()
	cfg2.SSLCertsDir = path.Join(tempDir, "temp2")
	cfg2.Addr = "127.0.0.1:0"
	require.NoError(t, os.Mkdir(cfg2.SSLCertsDir, 0755))
	require.NoError(t, cfg2.ValidateAddrs(ctx2))

	ctx3 := logtags.AddTag(ctx, "n", 3)
	cfg3 := &base.Config{}
	cfg3.InitDefaults()
	cfg3.SSLCertsDir = path.Join(tempDir, "temp3")
	cfg3.Addr = "127.0.0.1:0"
	require.NoError(t, os.Mkdir(cfg3.SSLCertsDir, 0755))
	require.NoError(t, cfg3.ValidateAddrs(ctx3))

	errReturned := make(chan error, 1)
	// Do a three-node handshake, and ensure no error is returned. The errors
	// returned should be nil, and one of the temp SSL certs directories should
	// not be empty.
	var addr1, addr2, addr3 string

	listener1, err := ListenAndUpdateAddrs(ctx1, &cfg1.Addr, &cfg1.AdvertiseAddr, "rpc")
	require.NoError(t, err)
	defer func() {
		_ = listener1.Close()
	}()
	addr1 = listener1.Addr().String()

	listener2, err := ListenAndUpdateAddrs(ctx2, &cfg2.Addr, &cfg2.AdvertiseAddr, "rpc")
	require.NoError(t, err)
	defer func() {
		_ = listener2.Close()
	}()
	addr2 = listener2.Addr().String()

	listener3, err := ListenAndUpdateAddrs(ctx3, &cfg3.Addr, &cfg3.AdvertiseAddr, "rpc")
	require.NoError(t, err)
	defer func() {
		_ = listener3.Close()
	}()
	addr3 = listener3.Addr().String()

	reporter := func(prefix string) func(string, ...interface{}) {
		return func(format string, args ...interface{}) {
			t.Logf(prefix+": "+format, args...)
		}
	}

	go func() {
		errReturned <- InitHandshake(ctx1, reporter("n1"), cfg1, "foobar", 3, []string{addr2, addr3}, cfg1.SSLCertsDir, listener1)
	}()
	go func() {
		errReturned <- InitHandshake(ctx2, reporter("n2"), cfg2, "foobarbaz", 3, []string{addr1, addr3}, cfg2.SSLCertsDir, listener2)
	}()
	go func() {
		errReturned <- InitHandshake(ctx3, reporter("n3"), cfg3, "foobar", 3, []string{addr1, addr2}, cfg3.SSLCertsDir, listener3)
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
