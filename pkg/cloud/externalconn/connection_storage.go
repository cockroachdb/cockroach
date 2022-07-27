// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package externalconn

import (
	"context"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/errors"
)

func makeExternalConnectionConfig(
	uri *url.URL, args cloud.ExternalStorageURIContext,
) (cloudpb.ExternalConnectionConfig, error) {
	externalConnCfg := cloudpb.ExternalConnectionConfig{}
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
	var ec *ExternalConnection
	if err := args.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		ec, err = LoadExternalConnection(ctx, cfg.Name, args.InternalExecutor,
			username.MakeSQLUsernameFromPreNormalizedString(cfg.User), txn)
		return err
	}); err != nil {
		return nil, errors.Wrap(err, "failed to load external connection object")
	}

	// Construct an ExternalStorage handle for the underlying resource represented
	// by the external connection object.
	details := ec.ConnectionDetails()
	connDetails, err := MakeConnectionDetails(ctx, *details)
	if err != nil {
		return nil, err
	}
	connection, err := connDetails.Dial(ctx, args, cfg.Path)
	if err != nil {
		return nil, errors.Wrap(err, "failed to Dial external connection")
	}

	var es cloud.ExternalStorage
	var ok bool
	if es, ok = connection.(cloud.ExternalStorage); !ok {
		return nil, errors.AssertionFailedf("cannot convert Connection to cloud.ExternalStorage")
	}

	return es, nil
}

func init() {
	scheme := "external"
	cloud.RegisterExternalStorageProvider(cloudpb.ExternalStorageProvider_external, parseExternalConnectionURL,
		makeExternalConnectionStorage, cloud.RedactedParams(), scheme)
}
