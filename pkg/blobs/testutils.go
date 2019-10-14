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
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/blobs/blobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/pkg/errors"
)

// TestBlobServiceClient can be used as a mock BlobClient
// in tests that use nodelocal storage
func TestBlobServiceClient(externalIODir string) (BlobClient, *stop.Stopper, error) {
	localNodeID := roachpb.NodeID(0)
	stopper := stop.NewStopper()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	s := rpc.NewServer(rpcContext)
	localBlobServer, err := NewBlobService(externalIODir)
	if err != nil {
		return nil, nil, err
	}
	blobspb.RegisterBlobServer(s, localBlobServer)
	ln, err := netutil.ListenAndServeGRPC(rpcContext.Stopper, s, util.TestAddr)
	if err != nil {
		return nil, nil, err
	}

	localDialer := nodedialer.New(rpcContext,
		func(nodeID roachpb.NodeID) (net.Addr, error) {
			if nodeID == localNodeID {
				return ln.Addr(), nil
			}
			return nil, errors.Errorf("node %d not found", nodeID)
		},
	)
	client, err := NewBlobClient(0, localDialer, externalIODir)
	return client, stopper, err
}

// TestEmptyBlobClient can be used as a mock BlobClient
// in tests that create ExternalStorage but do not use
// nodelocal storage
var TestEmptyBlobClient = &wrapperClient{self: 0}
