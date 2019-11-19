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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/pttracker"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptverifier"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// Config configures the Provider.
type Config struct {
	Settings         *cluster.Settings
	DB               *client.DB
	InternalExecutor sqlutil.InternalExecutor
}

type provider struct {
	protectedts.Storage
	protectedts.Verifier

	*pttracker.Tracker
}

// New creates a new protectedts.Provider.
func New(c Config) protectedts.Provider {
	s := ptstorage.New(c.Settings, c.InternalExecutor)
	v := ptverifier.New(c.DB, s)
	t := pttracker.New(c.Settings, c.DB, s)
	return &provider{
		Storage:  s,
		Tracker:  t,
		Verifier: v,
	}
}

func (p *provider) Start(ctx context.Context, stopper *stop.Stopper) error {
	return p.Tracker.Start(ctx, stopper)
}
