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
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/errors"
)

func parseAndValidateKafkaSinkURI(
	ctx context.Context, _ interface{}, _ username.SQLUsername, uri *url.URL,
) (externalconn.ExternalConnection, error) {
	// Validate the kafka URI by creating a kafka sink and throwing it away.
	//
	// TODO(adityamaru): When we add `CREATE EXTERNAL CONNECTION ... WITH` support
	// to accept JSONConfig we should validate that here too.
	_, err := makeKafkaSink(ctx, sinkURL{URL: uri}, changefeedbase.Targets{}, "",
		nil, nilMetricsRecorderBuilder, makeSinkTelemetryData())
	if err != nil {
		return nil, errors.Wrap(err, "invalid Kafka URI")
	}

	connDetails := connectionpb.ConnectionDetails{
		Provider: connectionpb.ConnectionProvider_kafka,
		Details: &connectionpb.ConnectionDetails_SimpleURI{
			SimpleURI: &connectionpb.SimpleURI{
				URI: uri.String(),
			},
		},
	}
	return externalconn.NewExternalConnection(connDetails), nil
}

func init() {
	externalconn.RegisterConnectionDetailsFromURIFactory(changefeedbase.SinkSchemeKafka,
		parseAndValidateKafkaSinkURI)
}
