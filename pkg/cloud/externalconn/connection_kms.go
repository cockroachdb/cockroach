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

// makeExternalConnectionKMS builds the KMS held by the named external
// connection. USAGE on the connection is checked at planning time, in
// sql.CheckDestinationPrivileges, and deliberately not here. A job that resumes
// replays the URIs persisted in its details instead of replanning, so a check
// on this path would fail a job that was authorized when it was planned.
// Anything that does replan, such as a scheduled backup, is checked again.
func makeExternalConnectionKMS(
	ctx context.Context, uri string, env cloud.KMSEnv,
) (cloud.KMS, error) {
	kmsURI, err := url.ParseRequestURI(uri)
	if err != nil {
		return nil, err
	}
	externalConnectionName, err := connectionNameFromURI(kmsURI)
	if err != nil {
		return nil, err
	}

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
