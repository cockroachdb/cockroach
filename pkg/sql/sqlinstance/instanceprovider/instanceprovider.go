// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package instanceprovider provides an implementation of the sqlinstance.provider interface.
package instanceprovider

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type writer interface {
	CreateInstance(ctx context.Context, sessionID sqlliveness.SessionID, sessionExpiration hlc.Timestamp, instanceAddr string, locality roachpb.Locality) (base.SQLInstanceID, error)
	ReleaseInstanceID(ctx context.Context, instanceID base.SQLInstanceID) error
}

// provider implements the sqlinstance.Provider interface for access to the
// sqlinstance subsystem.
type provider struct {
	*instancestorage.Reader
	storage      writer
	stopper      *stop.Stopper
	instanceAddr string
	session      sqlliveness.Instance
	locality     roachpb.Locality
	instanceID   base.SQLInstanceID
	sessionID    sqlliveness.SessionID
	started      bool
	stopped      syncutil.AtomicBool
}

// New constructs a new Provider.
func New(
	stopper *stop.Stopper,
	db *kv.DB,
	codec keys.SQLCodec,
	slProvider sqlliveness.Provider,
	addr string,
	locality roachpb.Locality,
	f *rangefeed.Factory,
	clock *hlc.Clock,
) sqlinstance.Provider {
	storage := instancestorage.NewStorage(db, codec, slProvider)
	reader := instancestorage.NewReader(storage, slProvider.CachedReader(), f, codec, clock, stopper)
	p := &provider{
		storage:      storage,
		stopper:      stopper,
		Reader:       reader,
		session:      slProvider,
		instanceAddr: addr,
		locality:     locality,
	}
	return p
}

// Start implements the sqlinstance.Provider interface.
func (p *provider) Start(ctx context.Context) error {
	if p.started {
		return errors.New("already started")
	}
	p.started = true

	{
		// Initialize the instance. We need to do this before starting the reader,
		// so that the reader sees the instance.
		ctx, cancel := p.stopper.WithCancelOnQuiesce(ctx)
		defer cancel()
		if err := p.init(ctx); err != nil {
			log.Ops.Warningf(ctx, "error creating SQL instance: %s", err)
			return err
		}
	}

	if err := p.Reader.Start(ctx); err != nil {
		return err
	}
	return nil
}

func (p *provider) init(ctx context.Context) error {
	session, err := p.session.Session(ctx)
	if err != nil {
		return errors.Wrap(err, "constructing session")
	}
	instanceID, err := p.storage.CreateInstance(ctx, session.ID(), session.Expiration(), p.instanceAddr, p.locality)
	if err != nil {
		return err
	}
	p.sessionID = session.ID()
	p.instanceID = instanceID

	session.RegisterCallbackForSessionExpiry(func(_ context.Context) {
		// Stop the instance asynchronously. This callback runs in a stopper task,
		// so it can't do the shutdown (as the shutdown stops the stopper).
		go func() {
			ctx, sp := p.stopper.Tracer().StartSpanCtx(context.Background(), "instance shutdown")
			defer sp.Finish()
			p.shutdownSQLInstance(ctx)
		}()
	})
	return nil
}

// ErrProviderShutDown is returned by Instance() if called after the instance ID
// has been released.
var ErrProviderShutDown = errors.New("instance provider shut down")

// Instance implements the sqlinstance.Provider interface.
func (p *provider) Instance(
	ctx context.Context,
) (_ base.SQLInstanceID, _ sqlliveness.SessionID, err error) {
	if !p.started {
		return base.SQLInstanceID(0), "", sqlinstance.NotStartedError
	}
	if p.stopped.Get() {
		return base.SQLInstanceID(0), "", ErrProviderShutDown
	}
	return p.instanceID, p.sessionID, nil
}

// shutdownSQLInstance releases the instance ID and stops the stopper.
func (p *provider) shutdownSQLInstance(ctx context.Context) {
	p.stopped.Set(true)
	err := p.storage.ReleaseInstanceID(ctx, p.instanceID)
	if err != nil {
		log.Ops.Warningf(ctx, "could not release instance id %d", p.instanceID)
	}
	p.stopper.Stop(ctx)
}
