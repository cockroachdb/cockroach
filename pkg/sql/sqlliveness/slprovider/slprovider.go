// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package slprovider exposes an implementation of the sqlliveness.Provider
// interface.
package slprovider

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// New constructs a new Provider.
//
// sessionEvents, if not nil, gets notified of some session state transitions.
func New(
	ambientCtx log.AmbientContext,
	stopper *stop.Stopper,
	clock *hlc.Clock,
	db *kv.DB,
	codec keys.SQLCodec,
	settings *cluster.Settings,
	testingKnobs *sqlliveness.TestingKnobs,
	sessionEvents slinstance.SessionEventListener,
) sqlliveness.Provider {
	storage := slstorage.NewStorage(ambientCtx, stopper, clock, db, codec, settings)
	instance := slinstance.NewSQLInstance(stopper, clock, storage, settings, testingKnobs, sessionEvents)
	return &provider{
		Storage:  storage,
		Instance: instance,
	}
}

type provider struct {
	*slstorage.Storage
	*slinstance.Instance
}

var _ sqlliveness.Provider = &provider{}

func (p *provider) Start(ctx context.Context, regionPhysicalRep []byte) {
	p.Storage.Start(ctx)
	p.Instance.Start(ctx, regionPhysicalRep)
}

func (p *provider) Metrics() metric.Struct {
	return p.Storage.Metrics()
}
