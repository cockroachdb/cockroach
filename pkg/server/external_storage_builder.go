// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	esAccessor *cloud.EarlyBootExternalStorageAccessor,
	conf base.ExternalIODirConfig,
	settings *cluster.Settings,
	nodeIDContainer *base.SQLIDContainer,
	nodeDialer *nodedialer.Dialer,
	testingKnobs base.TestingKnobs,
	allowLocalFastpath bool,
	db isql.DB,
	recorder multitenant.TenantSideExternalIORecorder,
	registry *metric.Registry,
	externalIODir string,
) {
	var blobClientFactory blobs.BlobClientFactory
	if p, ok := testingKnobs.Server.(*TestingKnobs); ok && p.BlobClientFactory != nil {
		blobClientFactory = p.BlobClientFactory
	}
	if blobClientFactory == nil {
		blobClientFactory = blobs.NewBlobClientFactory(nodeIDContainer, nodeDialer, externalIODir, allowLocalFastpath)
	}
	e.conf = conf
	e.settings = settings
	e.blobClientFactory = blobClientFactory
	e.initCalled = true
	e.db = db
	e.limiters = esAccessor.Limiters()
	e.recorder = recorder

	// Register the metrics that track interactions with external storage
	// providers.
	e.metrics = esAccessor.Metrics()
	registry.AddMetricStruct(e.metrics)
}

func (e *externalStorageBuilder) assertInitComplete() error {
	if !e.initCalled {
		return errors.AssertionFailedf("external storage not initialized")
	}
	return nil
}

func (e *externalStorageBuilder) makeExternalStorage(
	ctx context.Context, dest cloudpb.ExternalStorage, opts ...cloud.ExternalStorageOption,
) (cloud.ExternalStorage, error) {
	if !e.initCalled {
		return nil, errors.AssertionFailedf("cannot create external storage before init")
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
		return nil, errors.AssertionFailedf("cannot create external storage before init")
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
