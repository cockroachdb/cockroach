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
	"sync"

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
	"github.com/cockroachdb/logtags"
)

type writer interface {
	CreateInstance(ctx context.Context, sessionID sqlliveness.SessionID, sessionExpiration hlc.Timestamp, instanceAddr string, locality roachpb.Locality) (base.SQLInstanceID, error)
	ReleaseInstanceID(ctx context.Context, instanceID base.SQLInstanceID) error
}

// provider implements the sqlinstance.Provider interface for access to the sqlinstance subsystem.
type provider struct {
	*instancestorage.Reader
	storage      writer
	stopper      *stop.Stopper
	instanceAddr string
	session      sqlliveness.Instance
	locality     roachpb.Locality
	initOnce     sync.Once
	initialized  chan struct{}
	instanceID   base.SQLInstanceID
	sessionID    sqlliveness.SessionID
	initError    error
	mu           struct {
		syncutil.Mutex
		started bool
	}
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
		initialized:  make(chan struct{}),
	}
	return p
}

// Start implements the sqlinstance.Provider interface.
func (p *provider) Start(ctx context.Context) error {
	if p.started() {
		return p.initError
	}
	if err := p.Reader.Start(ctx); err != nil {
		p.initOnce.Do(func() {
			p.initError = err
			close(p.initialized)
		})
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.started = true
	return p.initError
}

func (p *provider) started() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mu.started
}

// Instance implements the sqlinstance.Provider interface.
func (p *provider) Instance(
	ctx context.Context,
) (_ base.SQLInstanceID, _ sqlliveness.SessionID, err error) {
	if !p.started() {
		return base.SQLInstanceID(0), "", sqlinstance.NotStartedError
	}

	p.maybeInitialize()
	select {
	case <-ctx.Done():
		return base.SQLInstanceID(0), "", ctx.Err()
	case <-p.stopper.ShouldQuiesce():
		return base.SQLInstanceID(0), "", stop.ErrUnavailable
	case <-p.initialized:
		if p.initError == nil {
			log.Ops.Infof(ctx, "created SQL instance %d", p.instanceID)
		} else {
			log.Ops.Warningf(ctx, "error creating SQL instance: %s", p.initError)
		}
		return p.instanceID, p.sessionID, p.initError
	}
}

func (p *provider) maybeInitialize() {
	p.initOnce.Do(func() {
		ctx := context.Background()
		if err := p.stopper.RunAsyncTask(ctx, "initialize-instance", func(ctx context.Context) {
			ctx = logtags.AddTag(ctx, "initialize-instance", nil)
			p.initError = p.initialize(ctx)
			close(p.initialized)
		}); err != nil {
			p.initError = err
			close(p.initialized)
		}
	})
}

func (p *provider) initialize(ctx context.Context) error {
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

// shutdownSQLInstance shuts down the SQL instance.
func (p *provider) shutdownSQLInstance(ctx context.Context) {
	if !p.started() {
		return
	}
	// Initialize initError if shutdownSQLInstance is called
	// before initialization of the instance ID
	go func() {
		p.initOnce.Do(func() {
			p.initError = errors.New("instance never initialized")
			close(p.initialized)
		})
	}()
	select {
	case <-ctx.Done():
		return
	case <-p.initialized:
	}
	// If there is any initialization error, return as there is nothing to do.
	if p.initError != nil {
		return
	}
	err := p.storage.ReleaseInstanceID(ctx, p.instanceID)
	if err != nil {
		log.Ops.Warningf(ctx, "could not release instance id %d", p.instanceID)
	}
	p.stopper.Stop(ctx)
}
