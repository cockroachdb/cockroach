// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package externalconn

import (
	"context"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/errors"
)

func makeExternalConnectionKMS(
	ctx context.Context, uri string, env cloud.KMSEnv,
) (cloud.KMS, error) {
	kmsURI, err := url.ParseRequestURI(uri)
	if err != nil {
		return nil, err
	}
	if kmsURI.Host == "" {
		return nil, errors.Newf("host component of an external URI must refer to an "+
			"existing External Connection object: %s", kmsURI.String())
	}
	externalConnectionName := kmsURI.Host

	// TODO(adityamaru): Use the `user` in `cfg` to perform privilege checks on
	// the external connection object we are about to retrieve.

	// Retrieve the external connection object from the system table.
	var ec ExternalConnection
	if err := env.DBHandle().Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		var err error
		ec, err = LoadExternalConnection(ctx, externalConnectionName, txn)
		return err
	}); err != nil {
		return nil, errors.Wrap(err, "failed to load external connection object")
	}

	// Sanity check that we are connecting to a KMS object.
	if ec.ConnectionType() != connectionpb.TypeKMS {
		return nil, errors.Newf("KMS cannot use object of type %s", ec.ConnectionType().String())
	}

	// Construct a KMS handle for the underlying resource represented by the
	// external connection object.
	switch d := ec.ConnectionProto().Details.(type) {
	case *connectionpb.ConnectionDetails_SimpleURI:
		return cloud.KMSFromURI(ctx, d.SimpleURI.URI, env)
	default:
		return nil, errors.Newf("cannot connect to %T; unsupported resource for a KMS connection", d)
	}
}

func init() {
	cloud.RegisterKMSFromURIFactory(makeExternalConnectionKMS, scheme)
}
