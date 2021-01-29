// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package ptprovider encapsulates the concrete implementation of the
// protectedts.Provider.
package ptprovider

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptcache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptreconcile"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptverifier"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// Config configures the Provider.
type Config struct {
	Settings             *cluster.Settings
	DB                   *kv.DB
	Stores               *kvserver.Stores
	ReconcileStatusFuncs ptreconcile.StatusFuncs
	InternalExecutor     sqlutil.InternalExecutor
}

type provider struct {
	protectedts.Storage
	protectedts.Verifier
	protectedts.Cache
}

// New creates a new protectedts.Provider.
func New(cfg Config) (protectedts.Provider, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	storage := ptstorage.New(cfg.Settings, cfg.InternalExecutor)
	verifier := ptverifier.New(cfg.DB, storage)
	return &provider{
		Storage: storage,
		Cache: ptcache.New(ptcache.Config{
			DB:       cfg.DB,
			Storage:  storage,
			Settings: cfg.Settings,
		}),
		Verifier: verifier,
	}, nil
}

func validateConfig(cfg Config) error {
	switch {
	case cfg.Settings == nil:
		return errors.Errorf("invalid nil Settings")
	case cfg.DB == nil:
		return errors.Errorf("invalid nil DB")
	case cfg.InternalExecutor == nil:
		return errors.Errorf("invalid nil InternalExecutor")
	default:
		return nil
	}
}

func (p *provider) Start(ctx context.Context, stopper *stop.Stopper) error {
	if cache, ok := p.Cache.(*ptcache.Cache); ok {
		return cache.Start(ctx, stopper)
	}
	return nil
}
