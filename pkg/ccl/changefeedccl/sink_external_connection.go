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
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/errors"
)

func makeExternalConnectionSink(
	ctx context.Context,
	u sinkURL,
	user username.SQLUsername,
	db *kv.DB,
	ie sqlutil.InternalExecutor,
	serverCfg *execinfra.ServerConfig,
	// TODO(cdc): Replace jobspb.ChangefeedDetails with ChangefeedConfig.
	feedCfg jobspb.ChangefeedDetails,
	timestampOracle timestampLowerBoundOracle,
	jobID jobspb.JobID,
	m metricsRecorder,
) (Sink, error) {
	if u.Host == "" {
		return nil, errors.Newf("host component of an external URI must refer to an "+
			"existing External Connection object: %s", u.String())
	}

	externalConnectionName := u.Host

	// Retrieve the external connection object from the system table.
	var ec externalconn.ExternalConnection
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		ec, err = externalconn.LoadExternalConnection(ctx, externalConnectionName, ie, txn)
		return err
	}); err != nil {
		return nil, errors.Wrap(err, "failed to load external connection object")
	}

	// Construct a Sink handle for the underlying resource represented by the
	// external connection object.
	switch d := ec.ConnectionProto().Details.(type) {
	case *connectionpb.ConnectionDetails_SimpleURI:
		// Replace the external connection URI in the `feedCfg` with the URI of the
		// underlying resource.
		feedCfg.SinkURI = d.SimpleURI.URI
		return getSink(ctx, serverCfg, feedCfg, timestampOracle, user, jobID, m)
	default:
		return nil, errors.Newf("cannot connect to %T; unsupported resource for a Sink connection", d)
	}
}
