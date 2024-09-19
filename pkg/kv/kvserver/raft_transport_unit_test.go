// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowdispatch"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/node_rac2"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestRaftTransportStartNewQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	st := cluster.MakeTestingClusterSettings()
	opts := rpc.DefaultContextOptions()
	opts.Insecure = true
	opts.ToleratedOffset = 500 * time.Millisecond
	opts.Stopper = stopper
	opts.Settings = st

	rpcC := rpc.NewContext(ctx, opts)

	rpcC.StorageClusterID.Set(context.Background(), uuid.MakeV4())

	// mrs := &dummyMultiRaftServer{}

	grpcServer, err := rpc.NewServer(ctx, rpcC)
	require.NoError(t, err)
	// RegisterMultiRaftServer(grpcServer, mrs)

	var addr net.Addr

	resolver := func(roachpb.NodeID) (net.Addr, roachpb.Locality, error) {
		if addr == nil {
			return nil, roachpb.Locality{}, errors.New("no addr yet") // should not happen in this test
		}
		return addr, roachpb.Locality{}, nil
	}

	tp := NewRaftTransport(
		log.MakeTestingAmbientCtxWithNewTracer(),
		cluster.MakeTestingClusterSettings(),
		stopper,
		hlc.NewClockForTesting(nil),
		nodedialer.New(rpcC, resolver),
		grpcServer,
		kvflowdispatch.NewDummyDispatch(),
		NoopStoresFlowControlIntegration{},
		NoopRaftTransportDisconnectListener{},
		(*node_rac2.AdmittedPiggybacker)(nil),
		nil, /* PiggybackedAdmittedResponseScheduler */
		nil, /* knobs */
	)

	ln, err := netutil.ListenAndServeGRPC(stopper, grpcServer, &util.UnresolvedAddr{NetworkField: "tcp", AddressField: "localhost:0"})
	if err != nil {
		t.Fatal(err)
	}

	addr = ln.Addr()

	defer func() {
		if ln != nil {
			_ = ln.Close()
		}
	}()

	if _, existingQueue := tp.getQueue(1, rpc.SystemClass); existingQueue {
		t.Fatal("queue already exists")
	}
	timeout := time.Duration(rand.Int63n(int64(5 * time.Millisecond)))
	log.Infof(ctx, "running test with a ctx cancellation of %s", timeout)
	ctxBoom, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		<-time.After(timeout)
		_ = ln.Close()
		ln = nil
		wg.Done()
	}()
	tp.startProcessNewQueue(ctxBoom, 1, rpc.SystemClass)

	wg.Wait()
}
