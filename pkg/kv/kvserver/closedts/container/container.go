// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package container

import (
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/minprop"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/provider"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/storage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/transport"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

// Config is a container that holds references to all of the components required
// to set up a full closed timestamp subsystem.
type Config struct {
	Settings *cluster.Settings
	Stopper  *stop.Stopper
	Clock    closedts.LiveClockFn
	Refresh  closedts.RefreshFn
	Dialer   closedts.Dialer
}

// A Container is a full closed timestamp subsystem along with the Config it was
// created from.
type Container struct {
	Config
	// Initialized on Start().
	Tracker  closedts.TrackerI
	Storage  closedts.Storage
	Provider closedts.Provider
	Server   ctpb.Server
	Clients  closedts.ClientRegistry

	nodeID        roachpb.NodeID
	delayedServer *delayedServer
	noop          bool // if true, is NoopContainer
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
// will need to be started separately, and will only be populated during Start().
//
// However, its RegisterClosedTimestampServer method can only be called before
// the Container is started.
func NewContainer(cfg Config) *Container {
	return &Container{
		Config: cfg,
	}
}

type delayedServer struct {
	active int32 // atomic
	s      ctpb.Server
}

func (s *delayedServer) Start() {
	atomic.StoreInt32(&s.active, 1)
}

func (s delayedServer) Get(client ctpb.ClosedTimestamp_GetServer) error {
	if atomic.LoadInt32(&s.active) == 0 {
		return errors.New("not available yet")
	}
	return s.s.Get(client)
}

// RegisterClosedTimestampServer registers the Server contained in the container
// with gRPC.
func (c *Container) RegisterClosedTimestampServer(s *grpc.Server) {
	c.delayedServer = &delayedServer{}
	ctpb.RegisterClosedTimestampServer(s, c.delayedServer)
}

// Start starts the Container. The Stopper used to create the Container is in
// charge of stopping it.
func (c *Container) Start(nodeID roachpb.NodeID) {
	cfg := c.Config

	if c.noop {
		return
	}

	storage := storage.NewMultiStorage(func() storage.SingleStorage {
		return storage.NewMemStorage(StorageBucketScale, storageBucketNum)
	})

	tracker := minprop.NewTracker()

	pConf := provider.Config{
		NodeID:   nodeID,
		Settings: cfg.Settings,
		Stopper:  cfg.Stopper,
		Storage:  storage,
		Clock:    cfg.Clock,
		Close:    closedts.AsCloseFn(tracker),
	}

	provider := provider.NewProvider(&pConf)

	server := transport.NewServer(cfg.Stopper, provider, cfg.Refresh)

	rConf := transport.Config{
		NodeID:   nodeID,
		Settings: cfg.Settings,
		Stopper:  cfg.Stopper,
		Dialer:   cfg.Dialer,
		Sink:     provider,
	}

	c.nodeID = nodeID
	c.Storage = storage
	c.Tracker = tracker
	c.Server = server
	c.Clients = transport.NewClients(rConf)
	c.Provider = provider
	c.Provider.Start()
	if c.delayedServer != nil {
		c.delayedServer.s = server
		c.delayedServer.Start()
	}
}
