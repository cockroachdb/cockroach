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
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant/multitenantio"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
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
	db                isql.DB
	limiters          cloud.Limiters
	recorder          multitenant.TenantSideExternalIORecorder
	metrics           metric.Struct
}

func (e *externalStorageBuilder) init(
	ctx context.Context,
	conf base.ExternalIODirConfig,
	settings *cluster.Settings,
	nodeIDContainer *base.NodeIDContainer,
	nodeDialer *nodedialer.Dialer,
	testingKnobs base.TestingKnobs,
	db isql.DB,
	recorder multitenant.TenantSideExternalIORecorder,
	registry *metric.Registry,
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
	e.db = db
	e.limiters = cloud.MakeLimiters(ctx, &settings.SV)
	e.recorder = recorder

	// Register the metrics that track interactions with external storage
	// providers.
	e.metrics = cloud.MakeMetrics()
	registry.AddMetricStruct(e.metrics)
}

func (e *externalStorageBuilder) makeExternalStorage(
	ctx context.Context, dest cloudpb.ExternalStorage, opts ...cloud.ExternalStorageOption,
) (cloud.ExternalStorage, error) {
	if !e.initCalled {
		return nil, errors.New("cannot create external storage before init")
	}
	return cloud.MakeExternalStorage(
		ctx, dest, e.conf, e.settings, e.blobClientFactory, e.db, e.limiters, e.metrics,
		append(e.defaultOptions(), opts...)...,
	)
}

func (e *externalStorageBuilder) makeExternalStorageFromURI(
	ctx context.Context, uri string, user username.SQLUsername, opts ...cloud.ExternalStorageOption,
) (cloud.ExternalStorage, error) {
	if !e.initCalled {
		return nil, errors.New("cannot create external storage before init")
	}
	return cloud.ExternalStorageFromURI(
		ctx, uri, e.conf, e.settings, e.blobClientFactory, user, e.db, e.limiters, e.metrics,
		append(e.defaultOptions(), opts...)...,
	)
}

func (e *externalStorageBuilder) defaultOptions() []cloud.ExternalStorageOption {
	bytesAllowedBeforeAccounting := multitenantio.DefaultBytesAllowedBeforeAccounting.Get(&e.settings.SV)
	return []cloud.ExternalStorageOption{
		cloud.WithIOAccountingInterceptor(multitenantio.NewReadWriteAccounter(e.recorder, bytesAllowedBeforeAccounting)),
	}
}
