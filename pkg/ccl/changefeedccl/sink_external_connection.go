// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/errors"
)

func makeExternalConnectionSink(
	ctx context.Context,
	u sinkURL,
	user username.SQLUsername,
	db *kv.DB,
	ie sqlutil.InternalExecutor,
	sinkCtx SinkContext,
) (Sink, error) {
	if u.Host == "" {
		return nil, errors.Newf("host component of an external URI must refer to an "+
			"existing External Connection object: %s", u.String())
	}

	externalConnectionName := u.Host

	// TODO(adityamaru): Use the `user` in `cfg` to perform privilege checks on
	// the external connection object we are about to retrieve.

	// Retrieve the external connection object from the system table.
	var ec *externalconn.ExternalConnection
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		ec, err = externalconn.LoadExternalConnection(ctx, externalConnectionName,
			connectionpb.TypeStorage, ie, user, txn)
		return err
	}); err != nil {
		return nil, errors.Wrap(err, "failed to load external connection object")
	}

	// Construct a Sink handle for the underlying resource represented by the
	// external connection object.
	details := ec.ConnectionDetails()
	connDetails, err := externalconn.MakeConnectionDetails(ctx, *details)
	if err != nil {
		return nil, err
	}
	connection, err := connDetails.Dial(ctx, sinkCtx, "" /* subdir */)
	if err != nil {
		return nil, errors.Wrap(err, "failed to Dial external connection")
	}

	var sink Sink
	var ok bool
	if sink, ok = connection.(Sink); !ok {
		return nil, errors.AssertionFailedf("cannot convert Connection to Sink")
	}

	return sink, nil
}
