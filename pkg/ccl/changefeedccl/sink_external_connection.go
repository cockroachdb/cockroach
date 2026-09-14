// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"maps"
	"net/url"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
	targets changefeedbase.Targets,
) (Sink, error) {
	uri, err := p.resolve(u.URL)
	if err != nil {
		return nil, err
	}
	// Replace the external connection URI in the `feedCfg` with the URI of the
	// underlying resource.
	feedCfg.SinkURI = uri.String()
	return getSink(ctx, serverCfg, feedCfg, timestampOracle, user, jobID, m, targets,
		false /* initialValidation */)
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
		jobspb.JobID(0), (*sliMetrics)(nil), changefeedbase.Targets{}, true /* initialValidation */)
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
		changefeedbase.SinkParamAzureAccessKeyCamel,
	))
}

// externalConnectionURIParams are the query parameters a changefeed may set
// on an external:// URI, so that one connection can serve feeds writing to
// different topics. A feed may set one only if the connection leaves it
// unset.
var externalConnectionURIParams = []string{
	changefeedbase.SinkParamTopicName,
	changefeedbase.SinkParamTopicPrefix,
}

type externalConnectionProvider interface {
	// resolve turns an external://<name>/<path>?<params> URI into the URI of the
	// underlying resource. The path is joined onto the stored URI's path. Each
	// parameter must be in externalConnectionURIParams and unset on the stored
	// URI, and is then added to it. If
	// changefeedbase.ExternalConnectionApplyURIPathAndParams is off, the path
	// and parameters are ignored.
	resolve(u *url.URL) (*url.URL, error)
}

type isqlExternalConnectionProvider struct {
	ctx      context.Context
	db       isql.DB
	settings *cluster.Settings
}

func makeExternalConnectionProvider(
	ctx context.Context, db isql.DB, settings *cluster.Settings,
) externalConnectionProvider {
	return &isqlExternalConnectionProvider{
		ctx:      ctx,
		db:       db,
		settings: settings,
	}
}

func (p *isqlExternalConnectionProvider) resolve(u *url.URL) (*url.URL, error) {
	if u.Host == "" {
		return nil, errors.Newf("host component of an external URI must refer to an "+
			"existing External Connection object: %s", u.String())
	}
	var ec externalconn.ExternalConnection
	if err := p.db.Txn(p.ctx, func(ctx context.Context, txn isql.Txn) error {
		var err error
		ec, err = externalconn.LoadExternalConnection(ctx, u.Host, txn)
		return err
	}); err != nil {
		return nil, errors.Wrap(err, "failed to load external connection object")
	}

	// Materialize leaves the stored URI untouched when handed a nil overlay, so
	// with the setting off the resolved URI is the stored one. When on, it is
	// handed the path alone; which parameters a feed may add is a changefeed
	// rule, applied below.
	var overlay *url.URL
	if changefeedbase.ExternalConnectionApplyURIPathAndParams.Get(&p.settings.SV) {
		overlay = &url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}
	}
	resolved, err := externalconn.Materialize(ec, overlay)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot connect to external connection %s", u.Host)
	}
	if overlay == nil || u.RawQuery == "" {
		return resolved, nil
	}

	params := u.Query()
	stored := resolved.Query()
	for _, key := range slices.Sorted(maps.Keys(params)) {
		if !slices.Contains(externalConnectionURIParams, key) {
			return nil, errors.WithHintf(errors.Newf("query parameter %s is not allowed in an "+
				"external URI, the external connection defines it", key),
				"a changefeed may set %s when the external connection leaves them unset",
				strings.Join(externalConnectionURIParams, ", "))
		}
		if stored.Has(key) {
			return nil, errors.Newf("query parameter %s is set by external connection %s "+
				"and cannot be overridden", key, u.Host)
		}
		for _, val := range params[key] {
			stored.Add(key, val)
		}
	}
	resolved.RawQuery = stored.Encode()
	return resolved, nil
}
