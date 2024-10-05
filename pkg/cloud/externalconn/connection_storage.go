// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package externalconn

import (
	"context"
	"net/url"
	"path"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/errors"
)

const scheme = "external"

func makeExternalConnectionConfig(
	uri *url.URL, args cloud.ExternalStorageURIContext,
) (cloudpb.ExternalStorage_ExternalConnectionConfig, error) {
	externalConnCfg := cloudpb.ExternalStorage_ExternalConnectionConfig{}
	if uri.Host == "" {
		return externalConnCfg, errors.Newf("host component of an external URI must refer to an "+
			"existing External Connection object: %s", uri.String())
	}
	if args.CurrentUser.Undefined() {
		return externalConnCfg, errors.Errorf("user creating the external connection storage must be specified")
	}
	normUser := args.CurrentUser.Normalized()
	externalConnCfg.User = normUser
	externalConnCfg.Name = uri.Host
	externalConnCfg.Path = uri.Path
	return externalConnCfg, nil
}

func parseExternalConnectionURL(
	args cloud.ExternalStorageURIContext, uri *url.URL,
) (cloudpb.ExternalStorage, error) {
	conf := cloudpb.ExternalStorage{}
	conf.Provider = cloudpb.ExternalStorageProvider_external
	var err error
	conf.ExternalConnectionConfig, err = makeExternalConnectionConfig(uri, args)
	return conf, err
}

func makeExternalConnectionStorage(
	ctx context.Context, args cloud.ExternalStorageContext, dest cloudpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	cfg := dest.ExternalConnectionConfig
	if cfg.Name == "" {
		return nil, errors.New("invalid ExternalConnectionConfig with an empty name")
	}

	// TODO(adityamaru): Use the `user` in `cfg` to perform privilege checks on
	// the external connection object we are about to retrieve.

	// Retrieve the external connection object from the system table.
	var ec ExternalConnection
	if err := args.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		var err error
		ec, err = LoadExternalConnection(ctx, cfg.Name, txn)
		return err
	}); err != nil {
		return nil, errors.Wrap(err, "failed to load external connection object")
	}

	// Sanity check that we are connecting to a STORAGE object.
	if ec.ConnectionType() != connectionpb.TypeStorage {
		return nil, errors.Newf("STORAGE cannot use object of type %s", ec.ConnectionType().String())
	}

	// Construct an ExternalStorage handle for the underlying resource represented
	// by the external connection object.
	switch d := ec.ConnectionProto().Details.(type) {
	case *connectionpb.ConnectionDetails_SimpleURI:
		// Append the subdirectory that was passed in with the `external` URI to the
		// underlying `nodelocal` URI.
		uri, err := url.Parse(d.SimpleURI.URI)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse `nodelocal` URI")
		}
		uri.Path = path.Join(uri.Path, cfg.Path)
		return cloud.ExternalStorageFromURI(ctx, uri.String(), args.IOConf, args.Settings,
			args.BlobClientFactory, username.MakeSQLUsernameFromPreNormalizedString(cfg.User),
			args.DB, args.Limiters, args.MetricsRecorder, args.Options...)
	default:
		return nil, errors.Newf("cannot connect to %T; unsupported resource for an ExternalStorage connection", d)
	}
}

func init() {
	cloud.RegisterExternalStorageProvider(cloudpb.ExternalStorageProvider_external,
		cloud.RegisteredProvider{
			ParseFn:        parseExternalConnectionURL,
			ConstructFn:    makeExternalConnectionStorage,
			RedactedParams: cloud.RedactedParams(),
			Schemes:        []string{scheme},
		})
}
