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
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/errors"
)

// externalStorageBuilder is a wrapper around the ExternalStorage factory
// methods. It allows us to separate the creation and initialization of the
// builder between NewServer() and Start() respectively.
// TODO(adityamaru): Consider moving this to pkg/cloud/impl at a future
// stage of the ongoing refactor.
type externalStorageBuilder struct {
	conf              base.ExternalIODirConfig
	settings          *cluster.Settings
	blobClientFactory blobs.BlobClientFactory
	initCalled        bool
	ie                *sql.InternalExecutor
	db                *kv.DB
	limiters          cloud.Limiters
	recorder          multitenant.TenantSideExternalIORecorder
}

func (e *externalStorageBuilder) init(
	ctx context.Context,
	conf base.ExternalIODirConfig,
	settings *cluster.Settings,
	nodeIDContainer *base.NodeIDContainer,
	nodeDialer *nodedialer.Dialer,
	testingKnobs base.TestingKnobs,
	ie *sql.InternalExecutor,
	db *kv.DB,
	recorder multitenant.TenantSideExternalIORecorder,
) {
	var blobClientFactory blobs.BlobClientFactory
	if p, ok := testingKnobs.Server.(*TestingKnobs); ok && p.BlobClientFactory != nil {
		blobClientFactory = p.BlobClientFactory
	}
	if blobClientFactory == nil {
		blobClientFactory = blobs.NewBlobClientFactory(nodeIDContainer, nodeDialer, settings.ExternalIODir)
	}
	e.conf = conf
	e.settings = settings
	e.blobClientFactory = blobClientFactory
	e.initCalled = true
	e.ie = ie
	e.db = db
	e.limiters = cloud.MakeLimiters(ctx, &settings.SV)
	e.recorder = recorder
}

func (e *externalStorageBuilder) makeExternalStorage(
	ctx context.Context, dest roachpb.ExternalStorage, opts ...cloud.ExternalStorageOption,
) (cloud.ExternalStorage, error) {
	if !e.initCalled {
		return nil, errors.New("cannot create external storage before init")
	}
	return cloud.MakeExternalStorage(ctx, dest, e.conf, e.settings, e.blobClientFactory, e.ie,
		e.db, e.limiters, append(e.defaultOptions(), opts...)...)
}

func (e *externalStorageBuilder) makeExternalStorageFromURI(
	ctx context.Context, uri string, user security.SQLUsername, opts ...cloud.ExternalStorageOption,
) (cloud.ExternalStorage, error) {
	if !e.initCalled {
		return nil, errors.New("cannot create external storage before init")
	}
	return cloud.ExternalStorageFromURI(ctx, uri, e.conf, e.settings, e.blobClientFactory, user, e.ie, e.db, e.limiters, append(e.defaultOptions(), opts...)...)
}

func (e *externalStorageBuilder) defaultOptions() []cloud.ExternalStorageOption {
	bytesAllowedBeforeAccounting := multitenant.DefaultBytesAllowedBeforeAccounting.Get(&e.settings.SV)
	return []cloud.ExternalStorageOption{
		cloud.WithIOAccountingInterceptor(multitenant.NewReadWriteAccounter(e.recorder, bytesAllowedBeforeAccounting)),
	}
}
