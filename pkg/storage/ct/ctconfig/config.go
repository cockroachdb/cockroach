// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ctconfig

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/ct"
	"github.com/cockroachdb/cockroach/pkg/storage/ct/ctpb"
	"github.com/cockroachdb/cockroach/pkg/storage/ct/ctstorage"
	"github.com/cockroachdb/cockroach/pkg/storage/ct/cttransport"
	"github.com/cockroachdb/cockroach/pkg/storage/ct/minprop"
	"github.com/cockroachdb/cockroach/pkg/storage/ct/provider"
	"github.com/cockroachdb/cockroach/pkg/util/stop"

	"google.golang.org/grpc"
)

// Config is a container that holds references to all of the components required
// to set up a full closed timestamp subsystem.
type Config struct {
	Settings *cluster.Settings
	Stopper  *stop.Stopper
	Clock    ct.LiveClockFn
	Refresh  ct.RefreshFn
	Dialer   ct.Dialer

	// GRPCServer specifies an (optional) server to register the CT update server
	// with.
	GRPCServer *grpc.Server
}

// A Container is a full closed timestamp subsystem along with the Config it was
// created from.
type Container struct {
	Config
	Tracker  *minprop.Tracker
	Storage  ct.Storage
	Provider ct.Provider
	Server   ctpb.ClosedTimestampServer
	Peers    ct.PeerRegistry
}

// NewContainer initializes a Container from the given Config. The Container
// will need to be started separately.
func NewContainer(cfg Config) *Container {
	storage := ctstorage.NewMultiStorage(func() ctstorage.SingleStorage {
		return ctstorage.NewMemStorage(5*ct.TargetDuration.Get(&cfg.Settings.SV), 2)
	})

	tracker := minprop.NewTracker()

	pConf := provider.Config{
		Settings: cfg.Settings,
		Stopper:  cfg.Stopper,
		Storage:  storage,
		Clock:    cfg.Clock,
		Close:    tracker.CloseFn(),
	}
	provider := provider.NewProvider(&pConf)

	server := cttransport.NewServer(cfg.Stopper, provider, cfg.Refresh)

	if cfg.GRPCServer != nil {
		ctpb.RegisterClosedTimestampServer(cfg.GRPCServer, server)
	}

	rConf := cttransport.Config{
		Settings: cfg.Settings,
		Stopper:  cfg.Stopper,
		Dialer:   cfg.Dialer,
		Sink:     provider,
	}

	return &Container{
		Config:   cfg,
		Storage:  storage,
		Provider: provider,
		Tracker:  tracker,
		Server:   server,
		Peers:    cttransport.NewClients(rConf),
	}
}

// Start starts the Container. The Stopper used to create the Container is in
// charge of stopping it.
func (c *Container) Start() {
	c.Provider.Start()
}

type dialerAdapter nodedialer.Dialer

func (da *dialerAdapter) Ready(nodeID roachpb.NodeID) bool {
	return (*nodedialer.Dialer)(da).GetCircuitBreaker(nodeID).Ready()
}

func (da *dialerAdapter) Dial(ctx context.Context, nodeID roachpb.NodeID) (ctpb.Client, error) {
	c, err := (*nodedialer.Dialer)(da).Dial(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	return ctpb.NewClosedTimestampClient(c).Get(ctx)
}

// DialerAdapter turns a node dialer into a ct.Dialer.
func DialerAdapter(dialer *nodedialer.Dialer) ct.Dialer {
	return (*dialerAdapter)(dialer)

}
