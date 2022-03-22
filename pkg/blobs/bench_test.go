// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package blobs

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/errors"
)

// filesize should be at least 1 GB when running these benchmarks.
// Reduced to 129 K for CI.
const filesize = 129 * 1 << 10

type benchmarkTestCase struct {
	localNodeID       roachpb.NodeID
	remoteNodeID      roachpb.NodeID
	localExternalDir  string
	remoteExternalDir string

	blobClient BlobClient
	fileSize   int64
	fileName   string
}

func writeLargeFile(t testing.TB, file string, size int64) {
	err := os.MkdirAll(filepath.Dir(file), 0755)
	if err != nil {
		t.Fatal(err)
	}
	content := make([]byte, size)
	err = ioutil.WriteFile(file, content, 0600)
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkStreamingReadFile(b *testing.B) {
	localNodeID := roachpb.NodeID(1)
	remoteNodeID := roachpb.NodeID(2)
	localExternalDir, remoteExternalDir, stopper, cleanUpFn := createTestResources(b)
	defer cleanUpFn()

	ctx := context.Background()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	factory := setUpService(b, rpcContext, localNodeID, remoteNodeID, localExternalDir, remoteExternalDir)
	blobClient, err := factory(ctx, remoteNodeID)
	if err != nil {
		b.Fatal(err)
	}
	params := &benchmarkTestCase{
		localNodeID:       localNodeID,
		remoteNodeID:      remoteNodeID,
		localExternalDir:  localExternalDir,
		remoteExternalDir: remoteExternalDir,
		blobClient:        blobClient,
		fileSize:          filesize,
		fileName:          "test/largefile.csv",
	}
	benchmarkStreamingReadFile(b, params)
}

func benchmarkStreamingReadFile(b *testing.B, tc *benchmarkTestCase) {
	writeLargeFile(b, filepath.Join(tc.remoteExternalDir, tc.fileName), tc.fileSize)
	writeTo := LocalStorage{externalIODir: tc.localExternalDir}
	b.ResetTimer()
	b.SetBytes(tc.fileSize)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		reader, _, err := tc.blobClient.ReadFile(ctx, tc.fileName, 0)
		if err != nil {
			b.Fatal(err)
		}
		w, err := writeTo.Writer(ctx, tc.fileName)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(w, ioctx.ReaderCtxAdapter(ctx, reader)); err != nil {
			b.Fatal(errors.CombineErrors(err, w.Close()))
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
		stat, err := writeTo.Stat(tc.fileName)
		if err != nil {
			b.Fatal(err)
		}
		if stat.Filesize != tc.fileSize {
			b.Fatal("incorrect number of bytes written")
		}
	}
}

func BenchmarkStreamingWriteFile(b *testing.B) {
	localNodeID := roachpb.NodeID(1)
	remoteNodeID := roachpb.NodeID(2)
	localExternalDir, remoteExternalDir, stopper, cleanUpFn := createTestResources(b)
	defer cleanUpFn()

	ctx := context.Background()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	factory := setUpService(b, rpcContext, localNodeID, remoteNodeID, localExternalDir, remoteExternalDir)
	blobClient, err := factory(ctx, remoteNodeID)
	if err != nil {
		b.Fatal(err)
	}
	params := &benchmarkTestCase{
		localNodeID:       localNodeID,
		remoteNodeID:      remoteNodeID,
		localExternalDir:  localExternalDir,
		remoteExternalDir: remoteExternalDir,
		blobClient:        blobClient,
		fileSize:          filesize,
		fileName:          "test/largefile.csv",
	}
	benchmarkStreamingWriteFile(b, params)
}

func benchmarkStreamingWriteFile(b *testing.B, tc *benchmarkTestCase) {
	content := make([]byte, tc.fileSize)
	b.ResetTimer()
	b.SetBytes(tc.fileSize)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		w, err := tc.blobClient.Writer(ctx, tc.fileName)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(w, bytes.NewReader(content)); err != nil {
			b.Fatal(errors.CombineErrors(w.Close(), err))
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
