// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/qos"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type internalServerFunc func(
	context.Context, *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error)

func (f internalServerFunc) Batch(
	ctx context.Context, ba *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	return f(ctx, ba)
}

func (f internalServerFunc) RangeFeed(
	_ *roachpb.RangeFeedRequest, _ roachpb.Internal_RangeFeedServer,
) error {
	panic("unimplemented")
}

// TestQosMiddleware tests that qos levels get properly transmitted.
func TestQosMiddleware(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Can't be zero because that'd be an empty offset.
	clock := hlc.NewClock(timeutil.Unix(0, 1).UnixNano, time.Nanosecond)
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	serverCtx := newTestContext(clusterID, clock, stopper)
	const serverNodeID = 1
	serverCtx.NodeID.Set(context.TODO(), serverNodeID)
	s := newTestServer(t, serverCtx,
		grpc.UnaryInterceptor(qosServerInterceptor(nil /* prevUnaryInterceptor */)))

	heartbeat := &ManualHeartbeatService{
		ready:              make(chan error),
		stopper:            stopper,
		clock:              clock,
		remoteClockMonitor: serverCtx.RemoteClocks,
		version:            serverCtx.version,
		nodeID:             &serverCtx.NodeID,
	}
	RegisterHeartbeatServer(s, heartbeat)
	type qosLevel struct {
		qos.Level
		ok bool
	}
	qosLevelChan := make(chan qosLevel, 1)
	batchFunc := internalServerFunc(func(
		ctx context.Context, ba *roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		l, ok := qos.LevelFromContext(ctx)
		qosLevelChan <- qosLevel{l, ok}
		return &roachpb.BatchResponse{}, nil
	})
	roachpb.RegisterInternalServer(s, batchFunc)

	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}
	remoteAddr := ln.Addr().String()

	clientCtx := newTestContext(clusterID, clock, stopper)
	// Make the interval shorter to speed up the test.
	clientCtx.heartbeatInterval = 1 * time.Millisecond
	go func() { heartbeat.ready <- nil }()
	conn, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Wait for the connection & successful heartbeat.
	testutils.SucceedsSoon(t, func() error {
		err := clientCtx.TestingConnHealth(remoteAddr, serverNodeID)
		if err != nil && err != ErrNotHeartbeated {
			t.Fatal(err)
		}
		return err
	})
	clientConn := roachpb.NewInternalClient(conn)

	t.Run("without", func(t *testing.T) {
		ctx := context.Background()
		if _, err = clientConn.Batch(ctx, &roachpb.BatchRequest{}); err != nil {
			t.Fatal(err)
		}
		got := <-qosLevelChan
		if got.ok {
			t.Fatalf("received context should not have contained a qos level")
		}
	})
	t.Run("with", func(t *testing.T) {
		l := qos.Level{Class: qos.ClassLow, Shard: 12}
		ctx := qos.ContextWithLevel(context.Background(), l)
		if _, err = clientConn.Batch(ctx, &roachpb.BatchRequest{}); err != nil {
			t.Fatal(err)
		}
		got := <-qosLevelChan
		if !got.ok {
			t.Fatalf("expected context to have contained a qos level")
		} else if got.Level != l {
			t.Fatalf("expected context to have qos level %v, got %v", l, got.Level)
		}
	})
	t.Run("malformed header", func(t *testing.T) {
		ctxWithMalformedHeader := metadata.AppendToOutgoingContext(context.Background(),
			clientQosLevelKey, "foo")
		var entry *log.Entry
		log.Intercept(ctxWithMalformedHeader, func(le log.Entry) {
			if entry == nil && le.Severity == log.Severity_ERROR {
				entry = &le
			}
		})
		if _, err = clientConn.Batch(ctxWithMalformedHeader, &roachpb.BatchRequest{}); err != nil {
			t.Fatal(err)
		}
		log.Intercept(ctxWithMalformedHeader, nil)
		got := <-qosLevelChan
		if got.ok {
			t.Fatalf("expected context to not have contained a qos level")
		}
		const msg = "malformed qos level c_qos header: "
		if entry == nil {
			t.Fatalf("expected a log entry to be captured")
		} else if !strings.Contains(entry.Message, msg) {
			t.Fatalf("Found log entry %q, expected a log entrying containing %q", entry.Message, msg)
		}
	})
	t.Run("extra headers", func(t *testing.T) {
		l1 := qos.Level{Class: qos.ClassLow, Shard: 1}
		l2 := qos.Level{Class: qos.ClassHigh, Shard: 2}
		ctxWithDuplicateInHeader := metadata.AppendToOutgoingContext(context.Background(),
			clientQosLevelKey, l1.EncodeString())
		ctxWithDuplicateInHeader = metadata.AppendToOutgoingContext(ctxWithDuplicateInHeader,
			clientQosLevelKey, l2.EncodeString())
		var entry *log.Entry
		log.Intercept(ctxWithDuplicateInHeader, func(le log.Entry) {
			if entry == nil && le.Severity == log.Severity_WARNING {
				entry = &le
			}
		})
		if _, err = clientConn.Batch(ctxWithDuplicateInHeader, &roachpb.BatchRequest{}); err != nil {
			t.Fatal(err)
		}
		log.Intercept(ctxWithDuplicateInHeader, nil)
		const msg = "unexpected multiple qos levels"
		if entry == nil {
			t.Fatalf("expected a log entry to be captured")
		} else if !strings.Contains(entry.Message, msg) {
			t.Fatalf("Found log entry %q, expected a log entrying containing %q", entry.Message, msg)
		}
		// Check that the used level corresponds to the first value, which in this
		// case is l1.
		got := <-qosLevelChan
		if !got.ok {
			t.Fatalf("expected context to have contained a qos level")
		} else if got.Level != l1 {
			t.Fatalf("expected context to have qos level %v, got %v", l1, got.Level)
		}
	})
}
