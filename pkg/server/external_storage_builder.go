// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// externalStorageBuilder is a wrapper around the ExternalStorage factory
// methods. It allows us to separate the creation and initialization of the
// builder between NewServer() and Start() respectively.
//
// TODO(adityamaru): Consider moving this to pkg/cloud/impl.
type externalStorageBuilder struct {
	conf              base.ExternalIODirConfig
	settings          *cluster.Settings
	blobClientFactory blobs.BlobClientFactory
	initCalled        bool
	ie                *sql.InternalExecutor
	db                *kv.DB
	limiters          cloud.Limiters
	mon               *mon.BytesMonitor
}

// externalStorageBuilderConfig contains the information needed to initialize an
// externalStorageBuilder.
type externalStorageBuilderConfig struct {
	conf            base.ExternalIODirConfig
	settings        *cluster.Settings
	nodeIDContainer *base.NodeIDContainer
	nodeDialer      *nodedialer.Dialer
	ie              *sql.InternalExecutor
	db              *kv.DB
	mon             *mon.BytesMonitor
	knobs           base.TestingKnobs
}

func (e *externalStorageBuilder) init(ctx context.Context, cfg *externalStorageBuilderConfig) {
	var blobClientFactory blobs.BlobClientFactory
	if p, ok := cfg.knobs.Server.(*TestingKnobs); ok && p.BlobClientFactory != nil {
		blobClientFactory = p.BlobClientFactory
	}
	if blobClientFactory == nil {
		blobClientFactory = blobs.NewBlobClientFactory(cfg.nodeIDContainer,
			cfg.nodeDialer, cfg.settings.ExternalIODir)
	}
	e.conf = cfg.conf
	e.settings = cfg.settings
	e.blobClientFactory = blobClientFactory
	e.initCalled = true
	e.ie = cfg.ie
	e.db = cfg.db
	e.limiters = cloud.MakeLimiters(ctx, &cfg.settings.SV)
	e.mon = cfg.mon
}

func (e *externalStorageBuilder) makeExternalStorage(
	ctx context.Context, dest roachpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	if !e.initCalled {
		return nil, errors.New("cannot create external storage before init")
	}
	return cloud.MakeExternalStorage(ctx, dest, e.conf, e.settings, e.blobClientFactory, e.ie,
		e.db, e.limiters, e.mon)
}

func (e *externalStorageBuilder) makeExternalStorageFromURI(
	ctx context.Context, uri string, user security.SQLUsername,
) (cloud.ExternalStorage, error) {
	if !e.initCalled {
		return nil, errors.New("cannot create external storage before init")
	}
	return cloud.ExternalStorageFromURI(ctx, uri, e.conf, e.settings, e.blobClientFactory, user,
		e.ie, e.db, e.limiters, e.mon)
}
