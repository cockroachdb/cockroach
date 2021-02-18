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
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestInitHandshake(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx, cancel := context.WithTimeout(context.TODO(), 70*time.Second)
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
	go func() {
		errReturned <- InitHandshake(ctx, cfg1, "foobar", []string{"localhost:8081", "localhost:8082"}, cfg1.SSLCertsDir, "localhost", "8080")
	}()
	go func() {
		errReturned <- InitHandshake(ctx, cfg2, "foobar", []string{"localhost:8080", "localhost:8082"}, cfg2.SSLCertsDir, "localhost", "8081")
	}()
	go func() {
		errReturned <- InitHandshake(ctx, cfg3, "foobar", []string{"localhost:8080", "localhost:8081"}, cfg3.SSLCertsDir, "localhost", "8082")
	}()

	count := 0
	for count < 3 {
		select {
		case err := <-errReturned:
			require.NoError(t, err)
			count++
		case <-ctx.Done():
			t.Fatal("test timeout exceeded")
		}
	}
	dirs := []string{cfg1.SSLCertsDir, cfg2.SSLCertsDir, cfg3.SSLCertsDir}
	found := false
	for _, dir := range dirs {
		f, err := os.Open(dir)
		require.NoError(t, err)
		_, err = f.Readdirnames(1)
		if err == nil {
			found = true
			break
		}
	}
	require.True(t, found)
}
