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

package container

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/minprop"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/provider"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/transport"
	"github.com/cockroachdb/cockroach/pkg/util/stop"

	"google.golang.org/grpc"
)

// Config is a container that holds references to all of the components required
// to set up a full closed timestamp subsystem.
type Config struct {
	NodeID   roachpb.NodeID
	Settings *cluster.Settings
	Stopper  *stop.Stopper
	Clock    closedts.LiveClockFn
	Refresh  closedts.RefreshFn
	Dialer   closedts.Dialer

	// GRPCServer specifies an (optional) server to register the CT update server
	// with.
	GRPCServer *grpc.Server
}

// A Container is a full closed timestamp subsystem along with the Config it was
// created from.
type Container struct {
	Config
	Tracker  *minprop.Tracker
	Storage  closedts.Storage
	Provider closedts.Provider
	Server   ctpb.Server
	Clients  closedts.ClientRegistry
}

const (
	// For each node, keep two historical buckets (i.e. one recent one, and one that
	// lagging followers can still satisfy some reads from).
	storageBucketNum = 2
	// StorageBucketScale determines the (exponential) spacing of storage buckets.
	// For example, a scale of 5s means that the second bucket will attempt to hold
	// a closed timestamp 5s in the past from the first, and the third 5*5=25s from
	// the first, etc.
	//
	// TODO(tschottdorf): it's straightforward to make this dynamic. It should track
	// the interval at which timestamps are closed out, ideally being a little shorter.
	// The effect of that would be that the most recent closed timestamp and the previous
	// one can be queried against separately.
	StorageBucketScale = 10 * time.Second
)

// NewContainer initializes a Container from the given Config. The Container
// will need to be started separately.
func NewContainer(cfg Config) *Container {
	storage := storage.NewMultiStorage(func() storage.SingleStorage {
		return storage.NewMemStorage(StorageBucketScale, storageBucketNum)
	})

	tracker := minprop.NewTracker()

	pConf := provider.Config{
		NodeID:   cfg.NodeID,
		Settings: cfg.Settings,
		Stopper:  cfg.Stopper,
		Storage:  storage,
		Clock:    cfg.Clock,
		Close:    tracker.CloseFn(),
	}
	provider := provider.NewProvider(&pConf)

	server := transport.NewServer(cfg.Stopper, provider, cfg.Refresh)

	if cfg.GRPCServer != nil {
		ctpb.RegisterClosedTimestampServer(cfg.GRPCServer, ctpb.ServerShim{Server: server})
	}

	rConf := transport.Config{
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
		Clients:  transport.NewClients(rConf),
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

// DialerAdapter turns a node dialer into a closedts.Dialer.
func DialerAdapter(dialer *nodedialer.Dialer) closedts.Dialer {
	return (*dialerAdapter)(dialer)
}

var _ closedts.Dialer = DialerAdapter(nil)
