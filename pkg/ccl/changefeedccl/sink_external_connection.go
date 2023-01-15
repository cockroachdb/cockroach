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

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
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

func validateExternalConnectionSinkURI(
	ctx context.Context, execCfg interface{}, user username.SQLUsername, uri string,
) error {

	serverCfg := execinfra.ServerConfig{ExternalStorageFromURI: func(ctx context.Context, uri string,
		user username.SQLUsername, opts ...cloud.ExternalStorageOption) (cloud.ExternalStorage, error) {
		return nil, nil
	}}

	// Pass through the server config, except for the WrapSink testing knob since that often assumes it's
	// inside a job.
	if actualExecCfg, ok := execCfg.(*sql.ExecutorConfig); ok {
		serverCfg = actualExecCfg.DistSQLSrv.ServerConfig
		if knobs, ok := serverCfg.TestingKnobs.Changefeed.(*TestingKnobs); ok && knobs.WrapSink != nil {
			wrapSink := knobs.WrapSink
			knobs.WrapSink = nil
			defer func() { knobs.WrapSink = wrapSink }()
		}
	}

	// Validate the URI by creating a canary sink.
	//
	// TODO(adityamaru): When we add `CREATE EXTERNAL CONNECTION ... WITH` support
	// to accept JSONConfig we should validate that here too.
	_, err := getSink(ctx, &serverCfg, jobspb.ChangefeedDetails{SinkURI: uri}, nil, user,
		jobspb.JobID(0), nil)
	if err != nil {
		return errors.Wrap(err, "invalid changefeed sink URI")
	}

	return nil
}

var supportedExternalConnectionTypes = map[string]connectionpb.ConnectionProvider{
	changefeedbase.SinkSchemeCloudStorageAzure:     connectionpb.ConnectionProvider_azure_storage,
	changefeedbase.SinkSchemeCloudStorageGCS:       connectionpb.ConnectionProvider_gs,
	GcpScheme:                                      connectionpb.ConnectionProvider_gcpubsub,
	changefeedbase.SinkSchemeCloudStorageHTTP:      connectionpb.ConnectionProvider_http,
	changefeedbase.SinkSchemeCloudStorageHTTPS:     connectionpb.ConnectionProvider_https,
	changefeedbase.SinkSchemeCloudStorageNodelocal: connectionpb.ConnectionProvider_nodelocal,
	changefeedbase.SinkSchemeCloudStorageS3:        connectionpb.ConnectionProvider_s3,
	changefeedbase.SinkSchemeKafka:                 connectionpb.ConnectionProvider_kafka,
	changefeedbase.SinkSchemeWebhookHTTP:           connectionpb.ConnectionProvider_webhookhttp,
	changefeedbase.SinkSchemeWebhookHTTPS:          connectionpb.ConnectionProvider_webhookhttps,
	// TODO (zinger): Not including SinkSchemeExperimentalSQL for now because A: it's undocumented
	// and B, in tests it leaks a *gosql.DB and I can't figure out why.
}

func init() {
	for scheme, provider := range supportedExternalConnectionTypes {
		externalconn.RegisterConnectionDetailsFromURIFactory(
			scheme,
			provider,
			externalconn.SimpleURIFactory,
		)

		externalconn.RegisterNamedValidation(
			scheme,
			`changefeed`,
			validateExternalConnectionSinkURI,
		)
	}
}
