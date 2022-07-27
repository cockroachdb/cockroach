// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package nodelocal

import (
	"context"
	"net/url"
	"path"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/errors"
)

var _ externalconn.ConnectionDetails = &localFileConnectionDetails{}

type localFileConnectionDetails struct {
	connectionpb.ConnectionDetails
}

// Dial implements the external.ConnectionDetails interface.
func (l *localFileConnectionDetails) Dial(
	ctx context.Context, args cloud.ExternalStorageContext, subdir string,
) (externalconn.Connection, error) {
	cfg := l.GetNodelocal().Cfg
	cfg.Path = path.Join(cfg.Path, subdir)
	externalStorageConf := cloudpb.ExternalStorage{
		Provider:        cloudpb.ExternalStorageProvider_nodelocal,
		LocalFileConfig: cfg,
	}
	es, err := cloud.MakeExternalStorage(ctx, externalStorageConf, args.IOConf, args.Settings,
		args.BlobClientFactory, args.InternalExecutor, args.DB, args.Limiters, args.Options...)
	if err != nil {
		return nil, errors.Wrap(err,
			"failed to construct `nodelocal` ExternalStorage while resolving external connection")
	}

	return es, nil
}

// ConnectionType implements the external.ConnectionDetails interface.
func (l *localFileConnectionDetails) ConnectionType() connectionpb.ConnectionType {
	return connectionpb.TypeStorage
}

// ConnectionProto implements the external.ConnectionDetails interface.
func (l *localFileConnectionDetails) ConnectionProto() *connectionpb.ConnectionDetails {
	return &l.ConnectionDetails
}

func parseLocalFileConnectionURI(
	_ context.Context, uri *url.URL,
) (connectionpb.ConnectionDetails, error) {
	connDetails := connectionpb.ConnectionDetails{
		Provider: connectionpb.ConnectionProvider_nodelocal,
		Details: &connectionpb.ConnectionDetails_Nodelocal{
			Nodelocal: &connectionpb.NodelocalConnectionDetails{},
		},
	}
	var err error
	connDetails.GetNodelocal().Cfg, err = makeLocalFileConfig(uri)
	return connDetails, err
}

func makeLocalFileConnectionDetails(
	_ context.Context, details connectionpb.ConnectionDetails,
) externalconn.ConnectionDetails {
	return &localFileConnectionDetails{ConnectionDetails: details}
}

func init() {
	const scheme = "nodelocal"
	externalconn.RegisterConnectionDetailsFromURIFactory(
		connectionpb.ConnectionProvider_nodelocal,
		scheme,
		parseLocalFileConnectionURI,
		makeLocalFileConnectionDetails,
	)
}
