// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/errors"
)

func makeExternalConnectionSink(
	ctx context.Context,
	u *changefeedbase.SinkURL,
	user username.SQLUsername,
	p externalConnectionProvider,
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

	uri, err := p.lookup(externalConnectionName)
	if err != nil {
		return nil, err
	}
	// Replace the external connection URI in the `feedCfg` with the URI of the
	// underlying resource.
	feedCfg.SinkURI = uri
	return getSink(ctx, serverCfg, feedCfg, timestampOracle, user, jobID, m)
}

func validateExternalConnectionSinkURI(
	ctx context.Context, env externalconn.ExternalConnEnv, uri string,
) error {

	serverCfg := &execinfra.ServerConfig{ExternalStorageFromURI: func(ctx context.Context, uri string,
		user username.SQLUsername, opts ...cloud.ExternalStorageOption) (cloud.ExternalStorage, error) {
		return nil, nil
	}}

	// Pass through the server config, except for the WrapSink testing knob since that often assumes it's
	// inside a job.

	if s, ok := env.ServerCfg.(*execinfra.ServerConfig); ok && s != nil {
		serverCfg = s
	}

	if knobs, ok := serverCfg.TestingKnobs.Changefeed.(*TestingKnobs); ok && knobs.WrapSink != nil {
		newCfg := *serverCfg
		newKnobs := *knobs
		newKnobs.WrapSink = nil
		newCfg.TestingKnobs.Changefeed = &newKnobs
		serverCfg = &newCfg
	}

	// Validate the URI by creating a canary sink.
	//
	// TODO(adityamaru): When we add `CREATE EXTERNAL CONNECTION ... WITH` support
	// to accept JSONConfig we should validate that here too.
	s, err := getSink(ctx, serverCfg, jobspb.ChangefeedDetails{SinkURI: uri}, nil, env.Username,
		jobspb.JobID(0), (*sliMetrics)(nil))
	if err != nil {
		return errors.Wrap(err, "invalid changefeed sink URI")
	}
	if err := s.Close(); err != nil {
		return errors.Wrap(err, "failed to close canary sink")
	}

	return nil
}

var supportedExternalConnectionTypes = map[string]connectionpb.ConnectionProvider{
	changefeedbase.SinkSchemeCloudStorageAzure:     connectionpb.ConnectionProvider_azure_storage,
	changefeedbase.SinkSchemeCloudStorageGCS:       connectionpb.ConnectionProvider_gs,
	GcpScheme:                                      connectionpb.ConnectionProvider_gcpubsub,
	changefeedbase.SinkSchemeCloudStorageHTTP:      connectionpb.ConnectionProvider_http,
	changefeedbase.SinkSchemeCloudStorageHTTPS:     connectionpb.ConnectionProvider_https,
	changefeedbase.DeprecatedSinkSchemeHTTP:        connectionpb.ConnectionProvider_http,
	changefeedbase.DeprecatedSinkSchemeHTTPS:       connectionpb.ConnectionProvider_https,
	changefeedbase.SinkSchemeCloudStorageNodelocal: connectionpb.ConnectionProvider_nodelocal,
	changefeedbase.SinkSchemeCloudStorageS3:        connectionpb.ConnectionProvider_s3,
	changefeedbase.SinkSchemeKafka:                 connectionpb.ConnectionProvider_kafka,
	changefeedbase.SinkSchemeWebhookHTTP:           connectionpb.ConnectionProvider_webhookhttp,
	changefeedbase.SinkSchemeWebhookHTTPS:          connectionpb.ConnectionProvider_webhookhttps,
	changefeedbase.SinkSchemeConfluentKafka:        connectionpb.ConnectionProvider_kafka,
	changefeedbase.SinkSchemeAzureKafka:            connectionpb.ConnectionProvider_kafka,
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

	cloud.RegisterRedactedParams(cloud.RedactedParams(
		changefeedbase.SinkParamSASLPassword,
		changefeedbase.SinkParamCACert,
		changefeedbase.SinkParamClientCert,
		changefeedbase.SinkParamClientKey,
		changefeedbase.SinkParamConfluentAPISecret,
		changefeedbase.SinkParamAzureAccessKey,
	))
}

type externalConnectionProvider interface {
	lookup(name string) (string, error)
}

type isqlExternalConnectionProvider struct {
	ctx context.Context
	db  isql.DB
}

func makeExternalConnectionProvider(ctx context.Context, db isql.DB) externalConnectionProvider {
	return &isqlExternalConnectionProvider{
		ctx: ctx,
		db:  db,
	}
}

func (p *isqlExternalConnectionProvider) lookup(name string) (string, error) {
	if name == "" {
		return "", errors.Newf("host component of an external URI must refer to an " +
			"existing External Connection object")
	}
	var ec externalconn.ExternalConnection
	if err := p.db.Txn(p.ctx, func(ctx context.Context, txn isql.Txn) error {
		var err error
		ec, err = externalconn.LoadExternalConnection(ctx, name, txn)
		return err
	}); err != nil {
		return "", errors.Wrap(err, "failed to load external connection object")
	}

	// Construct a Sink handle for the underlying resource represented by the
	// external connection object.
	switch d := ec.ConnectionProto().Details.(type) {
	case *connectionpb.ConnectionDetails_SimpleURI:
		return d.SimpleURI.URI, nil
	default:
		return "", errors.Newf("cannot connect to %T; unsupported resource for a changefeed connection", d)
	}
}
