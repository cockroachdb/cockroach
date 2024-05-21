// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storeliveness

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"google.golang.org/grpc"
)

// heartbeatSender is responsible for dialing a remote node, sending a heartbeat
// request to it, waiting for the response, and returning that response.
// Transport implements heartbeatSender, and each local store has a pointer to it.
type heartbeatSender interface {
	SendHeartbeat(req *storelivenesspb.HeartbeatRequest) (*storelivenesspb.HeartbeatResponse, error)
}

// heartbeatHandler is responsible for taking a heartbeat request from a remote
// node, delegating its handling to a local store, and returning the resulting
// heartbeat response.
// Each local store's SupportManager implements heartbeatHandler, and Transport
// has a map of heartbeatHandlers, one per local store.
type heartbeatHandler interface {
	HandleHeartbeat(req *storelivenesspb.HeartbeatRequest) (*storelivenesspb.HeartbeatResponse, error)
}

type Transport struct {
	clock   *hlc.Clock
	stopper *stop.Stopper
	dialer  *nodedialer.Dialer
	mu      syncutil.RWMutex
	// Protected by mu.
	heartbeatHandlers map[storelivenesspb.StoreIdent]*heartbeatHandler
}

func NewTransport(
	clock *hlc.Clock,
	dialer *nodedialer.Dialer,
	grpcServer *grpc.Server,
	stopper *stop.Stopper,
) *Transport {
	t := &Transport{
		clock:             clock,
		stopper:           stopper,
		dialer:            dialer,
		heartbeatHandlers: map[storelivenesspb.StoreIdent]*heartbeatHandler{},
	}
	if grpcServer != nil {
		storelivenesspb.RegisterStoreLivenessServer(grpcServer, t)
	}
	return t
}

func (t *Transport) SetHeartbeatHandler(storeID storelivenesspb.StoreIdent, hbh heartbeatHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.heartbeatHandlers[storeID] = &hbh
}

// Heartbeat implements the StoreLiveness grpc service interface.
func (t *Transport) Heartbeat(
	ctx context.Context, req *storelivenesspb.HeartbeatRequest,
) (*storelivenesspb.HeartbeatResponse, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	header := storelivenesspb.Header{
		From:       req.Header.To,
		To:         req.Header.From,
		SenderTime: t.clock.NowAsClockTimestamp(),
	}
	toStore := req.Header.To
	hbh, ok := t.heartbeatHandlers[toStore]
	if !ok {
		log.Infof(context.Background(), "handling a heartbeat request to a missing local store %+v", toStore)
		return &storelivenesspb.HeartbeatResponse{
			Header: header, HeartbeatAck: false,
		}, nil
	}
	res, err := (*hbh).HandleHeartbeat(req)
	if err != nil {
		return &storelivenesspb.HeartbeatResponse{
			Header: header, HeartbeatAck: false,
		}, err
	}
	return res, nil
}

// SendHeartbeat creates a client to the StoreLiveness grpc service and
// implements the heartbeatSender interface.
func (t *Transport) SendHeartbeat(req *storelivenesspb.HeartbeatRequest,
) (*storelivenesspb.HeartbeatResponse, error) {
	ctx := context.Background()
	conn, err := t.dialer.Dial(ctx, req.Header.To.NodeID, rpc.DefaultClass)
	if err != nil {
		return nil, err
	}
	client := storelivenesspb.NewStoreLivenessClient(conn)
	return client.Heartbeat(ctx, req)
}
