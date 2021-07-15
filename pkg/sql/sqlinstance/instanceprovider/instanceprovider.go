// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type writer interface {
	CreateInstance(context.Context, sqlliveness.SessionID, string) (base.SQLInstanceID, error)
	ReleaseInstanceID(context.Context, base.SQLInstanceID) error
}

// provider implements the sqlinstance.Provider interface for access to the sqlinstance subsystem.
type provider struct {
	sqlinstance.AddressResolver
	storage writer
	stopper *stop.Stopper
	mu      struct {
		syncutil.Mutex
		instanceAddr string
		instanceInfo *sqlinstance.InstanceInfo
		session      sqlliveness.Instance
	}
}

// New constructs a new Provider.
func New(
	stopper *stop.Stopper,
	db *kv.DB,
	codec keys.SQLCodec,
	slProvider sqlliveness.Provider,
	addr string,
) sqlinstance.Provider {
	storage := instancestorage.NewStorage(db, codec, slProvider)
	reader := instancestorage.NewReader(storage, slProvider)
	p := &provider{
		storage:         storage,
		stopper:         stopper,
		AddressResolver: reader,
	}
	p.mu.session = slProvider
	p.mu.instanceAddr = addr
	return p
}

// Instance returns the instance ID for the current SQL instance.
func (p *provider) Instance(ctx context.Context) (base.SQLInstanceID, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.stopper.ShouldQuiesce():
		return base.SQLInstanceID(0), stop.ErrUnavailable
	case <-ctx.Done():
		return base.SQLInstanceID(0), ctx.Err()
	default:
	}
	if p.mu.instanceInfo == nil {
		// Create a new instance if instanceInfo has not been initialized.
		session, err := p.mu.session.Session(ctx)
		if err != nil {
			return base.SQLInstanceID(0), err
		}
		instanceID, err := p.storage.CreateInstance(ctx, session.ID(), p.mu.instanceAddr)
		if err != nil {
			return base.SQLInstanceID(0), err
		}
		p.mu.instanceInfo = sqlinstance.NewSQLInstanceInfo(instanceID, p.mu.instanceAddr, session.ID())
		session.RegisterCallbackForSessionExpiry("ShutdownSQLInstance", p.ShutdownSQLInstance)
	}
	log.Infof(ctx, "created SQL instance %d", p.mu.instanceInfo.InstanceID())
	return p.mu.instanceInfo.InstanceID(), nil
}

// ShutdownSQLInstance shuts down the SQL instance.
func (p *provider) ShutdownSQLInstance(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	err := p.storage.ReleaseInstanceID(ctx, p.mu.instanceInfo.InstanceID())
	if err != nil {
		log.Warningf(ctx, "could not release instance id %d", p.mu.instanceInfo.InstanceID())
	}
	p.stopper.Stop(ctx)
}
