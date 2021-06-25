// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package instanceprovider provides an implementation
// of the sqlinstance.provider interface
package instanceprovider

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type writer interface {
	CreateInstance(context.Context, sqlliveness.SessionID, string) (sqlinstance.SQLInstance, error)
	ReleaseInstanceID(context.Context, base.SQLInstanceID) error
}
type storage interface {
	sqlinstance.AddressResolver
	writer
	Start()
}

type provider struct {
	storage
	stopper *stop.Stopper
	mu      struct {
		syncutil.Mutex
		started  bool
		session  sqlliveness.Instance
		httpAddr string
		instance sqlinstance.SQLInstance
	}
}

// New constructs a new Provider.
func New(
	stopper *stop.Stopper,
	db *kv.DB,
	codec keys.SQLCodec,
	session sqlliveness.Instance,
	httpAddr string,
) sqlinstance.Provider {
	storage := instancestorage.NewStorage(stopper, db, codec)
	p := &provider{
		storage: storage,
		stopper: stopper,
	}
	p.mu.session = session
	p.mu.httpAddr = httpAddr
	return p
}

// Start starts the sqlinstance.provider.
func (p *provider) Start(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mu.started {
		return
	}
	p.mu.started = true
	log.Info(ctx, "starting sqlinstance subsystem")
	p.storage.Start()
}

// Instance returns the sqlinstance.Instance associated with the provider.
func (p *provider) Instance(ctx context.Context) (_ sqlinstance.SQLInstance, err error) {
	if err = p.checkStarted(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.stopper.ShouldQuiesce():
		return nil, stop.ErrUnavailable
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if p.mu.instance == nil {
		var session sqlliveness.Session
		session, err = p.mu.session.Session(ctx)
		if err != nil {
			return nil, err
		}
		p.mu.instance, err = p.storage.CreateInstance(ctx, session.ID(), p.mu.httpAddr)
		if err != nil {
			return nil, err
		}
		session.RegisterCallbackForSessionExpiry(p.ShutdownSQLInstance)
	}
	log.Infof(ctx, "created SQL instance %d", p.mu.instance.InstanceID())
	return p.mu.instance, nil
}

func (p *provider) checkStarted() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.mu.started {
		return sqlinstance.NotStartedError
	}
	return nil
}

// ShutdownSQLInstance shuts down the sqlinstance.Instance.
func (p *provider) ShutdownSQLInstance(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	err := p.storage.ReleaseInstanceID(ctx, p.mu.instance.InstanceID())
	if err != nil {
		log.Warningf(ctx, "could not release instance id %d", p.mu.instance.InstanceID())
	}
	p.stopper.Stop(ctx)
}
