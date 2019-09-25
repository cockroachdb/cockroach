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
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/blobs/blobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

func setUpService(
	t *testing.T,
	rpcContext *rpc.Context,
	localNodeID roachpb.NodeID,
	remoteNodeID roachpb.NodeID,
	localExternalDir string,
	remoteExternalDir string,
) *Client {
	s := rpc.NewServer(rpcContext)
	remoteBlobServer := NewBlobService(remoteExternalDir)
	blobspb.RegisterBlobServer(s, remoteBlobServer)
	ln, err := netutil.ListenAndServeGRPC(rpcContext.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}

	s2 := rpc.NewServer(rpcContext)
	localBlobServer := NewBlobService(localExternalDir)
	blobspb.RegisterBlobServer(s2, localBlobServer)
	ln2, err := netutil.ListenAndServeGRPC(rpcContext.Stopper, s2, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}

	localDialer := nodedialer.New(rpcContext,
		func(nodeID roachpb.NodeID) (net.Addr, error) {
			if nodeID == remoteNodeID {
				return ln.Addr(), nil
			} else if nodeID == localNodeID {
				return ln2.Addr(), nil
			}
			return nil, errors.Errorf("node %d not found", nodeID)
		},
	)
	return NewBlobClient(localDialer, localNodeID)
}

func writeTestFile(t *testing.T, file string, content []byte) {
	err := os.MkdirAll(filepath.Dir(file), 0755)
	if err != nil {
		t.Fatal(err)
	}
	err = ioutil.WriteFile(file, content, 0600)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBlobClientReadFile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	localExternalDir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()
	localNodeID := roachpb.NodeID(1)

	remoteExternalDir, cleanupFn2 := testutils.TempDir(t)
	defer cleanupFn2()
	remoteNodeID := roachpb.NodeID(2)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	blobClient := setUpService(t, rpcContext, localNodeID, remoteNodeID, localExternalDir, remoteExternalDir)
	// Check that local client ID is correct
	if blobClient.GetNodeID() != localNodeID {
		t.Fatalf("expected local node id: %d got: %d", localNodeID, blobClient.GetNodeID())
	}

	localFileContent := []byte("local_file")
	remoteFileContent := []byte("remote_file")
	writeTestFile(t, filepath.Join(localExternalDir, "test/local.csv"), localFileContent)
	writeTestFile(t, filepath.Join(remoteExternalDir, "test/remote.csv"), remoteFileContent)

	for _, tc := range []struct {
		name        string
		nodeID      roachpb.NodeID
		filename    string
		fileContent []byte
	}{
		{
			"fetch-remote-file",
			remoteNodeID,
			"test/remote.csv",
			remoteFileContent,
		},
		{
			"fetch-local-file",
			localNodeID,
			"test/local.csv",
			localFileContent,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.TODO()
			reader, err := blobClient.ReadFile(ctx, tc.nodeID, tc.filename)
			if err != nil {
				t.Fatal(err)
			}
			// Check that fetched file content is correct
			content, err := ioutil.ReadAll(reader)
			if err != nil {
				t.Fatal(err, "unable to read fetched file")
			}
			if !bytes.Equal(content, tc.fileContent) {
				t.Fatal(fmt.Sprintf(`fetched file content incorrect, expected %s, got %s`, tc.fileContent, content))
			}
		})
	}
}

func TestBlobClientWriteFile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	localExternalDir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()
	localNodeID := roachpb.NodeID(1)

	remoteExternalDir, cleanupFn2 := testutils.TempDir(t)
	defer cleanupFn2()
	remoteNodeID := roachpb.NodeID(2)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	blobClient := setUpService(t, rpcContext, localNodeID, remoteNodeID, localExternalDir, remoteExternalDir)

	for _, tc := range []struct {
		name               string
		nodeID             roachpb.NodeID
		filename           string
		fileContent        string
		destinationNodeDir string
	}{
		{
			"send-remote-file",
			remoteNodeID,
			"test/remote.csv",
			"remotefile",
			remoteExternalDir,
		},
		{
			"send-local-file",
			localNodeID,
			"test/local.csv",
			"localfile",
			localExternalDir,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.TODO()
			byteContent := []byte(tc.fileContent)
			err := blobClient.WriteFile(ctx, tc.nodeID, tc.filename, bytes.NewReader(byteContent))
			if err != nil {
				t.Fatal(err)
			}
			// Check that file is now in correct node
			content, err := ioutil.ReadFile(filepath.Join(tc.destinationNodeDir, tc.filename))
			if err != nil {
				t.Fatal(err, "unable to read fetched file")
			}
			if !bytes.Equal(content, byteContent) {
				t.Fatal(fmt.Sprintf(`fetched file content incorrect, expected %s, got %s`, tc.fileContent, content))
			}
		})
	}
}

func TestBlobClientList(t *testing.T) {
	defer leaktest.AfterTest(t)()

	localExternalDir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()
	localNodeID := roachpb.NodeID(1)

	remoteExternalDir, cleanupFn2 := testutils.TempDir(t)
	defer cleanupFn2()
	remoteNodeID := roachpb.NodeID(2)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	blobClient := setUpService(t, rpcContext, localNodeID, remoteNodeID, localExternalDir, remoteExternalDir)

	localFileNames := []string{"file/local/dataA.csv", "file/local/dataB.csv", "file/local/dataC.csv"}
	remoteFileNames := []string{"file/remote/A.csv", "file/remote/B.csv", "file/remote/C.csv"}
	var expectedLocalList, expectedRemoteList []string
	for _, fileName := range localFileNames {
		fullPath := filepath.Join(localExternalDir, fileName)
		writeTestFile(t, fullPath, []byte("testLocalFile"))
		expectedLocalList = append(expectedLocalList, fullPath)
	}
	for _, fileName := range remoteFileNames {
		fullPath := filepath.Join(remoteExternalDir, fileName)
		writeTestFile(t, fullPath, []byte("testRemoteFile"))
		expectedRemoteList = append(expectedRemoteList, fullPath)
	}

	for _, tc := range []struct {
		name         string
		nodeID       roachpb.NodeID
		dirName      string
		expectedList []string
	}{
		{
			"fetch-list-local",
			localNodeID,
			"file/local/*.csv",
			expectedLocalList,
		},
		{
			"fetch-list-remote",
			remoteNodeID,
			"file/remote/*.csv",
			expectedRemoteList,
		},
		{
			"fetch-list-local-no-match",
			localNodeID,
			"file/doesnotexist/*",
			[]string{},
		},
		{
			"fetch-list-remote-no-match",
			remoteNodeID,
			"file/doesnotexist/*",
			[]string{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.TODO()
			list, err := blobClient.List(ctx, tc.nodeID, tc.dirName)
			if err != nil {
				t.Fatal(err)
			}
			// Check that returned list matches expected list
			if len(list) != len(tc.expectedList) {
				t.Fatal(`listed incorrect number of files`, list)
			}
			for i, f := range list {
				if f != tc.expectedList[i] {
					t.Fatal("incorrect list returned ", list)
				}
			}
		})
	}
}

func TestBlobClientDeleteFrom(t *testing.T) {
	defer leaktest.AfterTest(t)()

	localExternalDir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()
	localNodeID := roachpb.NodeID(1)

	remoteExternalDir, cleanupFn2 := testutils.TempDir(t)
	defer cleanupFn2()
	remoteNodeID := roachpb.NodeID(2)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	blobClient := setUpService(t, rpcContext, localNodeID, remoteNodeID, localExternalDir, remoteExternalDir)

	localFileContent := []byte("local_file")
	remoteFileContent := []byte("remote_file")
	writeTestFile(t, filepath.Join(localExternalDir, "test/local.csv"), localFileContent)
	writeTestFile(t, filepath.Join(remoteExternalDir, "test/remote.csv"), remoteFileContent)

	for _, tc := range []struct {
		name     string
		nodeID   roachpb.NodeID
		filename string
		err      string
	}{
		{
			"delete-remote-file",
			remoteNodeID,
			"test/remote.csv",
			"",
		},
		{
			"delete-local-file",
			localNodeID,
			"test/local.csv",
			"",
		},
		{
			"delete-remote-file-does-not-exist",
			remoteNodeID,
			"test/doesnotexist",
			"no such file",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.TODO()
			err := blobClient.DeleteFrom(ctx, tc.nodeID, tc.filename)
			if err != nil {
				if tc.err != "" && strings.Contains(err.Error(), tc.err) {
					// the correct error was returned
					return
				}
				t.Fatal(err)
			}

			_, err = ioutil.ReadFile(filepath.Join(localExternalDir, tc.filename))
			if err == nil {
				t.Fatal(err, "file should have been deleted")
			}
		})
	}
}

func TestBlobClientStat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	localExternalDir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()
	localNodeID := roachpb.NodeID(1)

	remoteExternalDir, cleanupFn2 := testutils.TempDir(t)
	defer cleanupFn2()
	remoteNodeID := roachpb.NodeID(2)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	blobClient := setUpService(t, rpcContext, localNodeID, remoteNodeID, localExternalDir, remoteExternalDir)

	localFileContent := []byte("local_file")
	remoteFileContent := []byte("remote_file")
	writeTestFile(t, filepath.Join(localExternalDir, "test/local.csv"), localFileContent)
	writeTestFile(t, filepath.Join(remoteExternalDir, "test/remote.csv"), remoteFileContent)

	for _, tc := range []struct {
		name         string
		nodeID       roachpb.NodeID
		filename     string
		expectedSize int64
		err          string
	}{
		{
			"size-remote-file",
			remoteNodeID,
			"test/remote.csv",
			int64(len(remoteFileContent)),
			"",
		},
		{
			"size-local-file",
			localNodeID,
			"test/local.csv",
			int64(len(localFileContent)),
			"",
		},
		{
			"size-remote-file-does-not-exist",
			remoteNodeID,
			"test/doesnotexist",
			0,
			"no such file",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.TODO()
			resp, err := blobClient.Stat(ctx, tc.nodeID, tc.filename)
			if err != nil {
				if tc.err != "" && strings.Contains(err.Error(), tc.err) {
					// the correct error was returned
					return
				}
				t.Fatal(err)
			}
			if resp.Filesize != tc.expectedSize {
				t.Fatalf("expected size: %d got: %d", tc.expectedSize, resp)
			}
		})
	}
}
