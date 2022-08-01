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
	"github.com/cockroachdb/errors"
)

func parseAndValidateKafkaSinkURI(
	ctx context.Context, uri *url.URL,
) (connectionpb.ConnectionDetails, error) {
	// Validate the kafka URI by creating a kafka sink.
	//
	// TODO(adityamaru): Add `CREATE EXTERNAL CONNECTION ... WITH` support to
	// accept JSONConfig.
	_, err := makeKafkaSink(ctx, sinkURL{URL: uri}, changefeedbase.Targets{}, "", nilMetricsRecorderBuilder)
	if err != nil {
		return connectionpb.ConnectionDetails{}, errors.Wrap(err, "invalid Kafka URI")
	}

	connDetails := connectionpb.ConnectionDetails{
		Provider: connectionpb.ConnectionProvider_kafka,
		Details: &connectionpb.ConnectionDetails_Kafka{
			Kafka: &connectionpb.KafkaConnectionDetails{
				URI: uri.String(),
			},
		},
	}
	return connDetails, nil
}

type kafkaConnectionDetails struct {
	connectionpb.ConnectionDetails
}

// Dial implements the ConnectionDetails interface.
func (k *kafkaConnectionDetails) Dial(
	ctx context.Context, connectionCtx interface{}, _ string,
) (externalconn.Connection, error) {
	sinkCtx, ok := connectionCtx.(SinkContext)
	if !ok {
		return nil, errors.Newf("Kafka sink dialed with an incompatible context of type %T", connectionCtx)
	}
	uri, err := url.Parse(k.GetKafka().URI)
	if err != nil {
		return nil, errors.New("failed to parse kafka URI when dialing external connection")
	}

	// TODO(adityamaru): Currently, we're getting the kafkaJSONConfig from the
	// `CREATE CHANGEFEED` statement but we might consider moving this option to a
	// `CREATE EXTERNAL CONNECTION` option in the future.
	return makeKafkaSink(ctx, sinkURL{URL: uri}, sinkCtx.targets, sinkCtx.kafkaJSONConfig, sinkCtx.mb)
}

// ConnectionProto implements the ConnectionDetails interface.
func (k *kafkaConnectionDetails) ConnectionProto() *connectionpb.ConnectionDetails {
	return &k.ConnectionDetails
}

// ConnectionType implements the ConnectionDetails interface.
func (k *kafkaConnectionDetails) ConnectionType() connectionpb.ConnectionType {
	return k.ConnectionDetails.Type()
}

var _ externalconn.ConnectionDetails = &kafkaConnectionDetails{}

func makeKafkaSinkConnectionDetails(
	_ context.Context, details connectionpb.ConnectionDetails,
) externalconn.ConnectionDetails {
	return &kafkaConnectionDetails{ConnectionDetails: details}
}

func init() {
	externalconn.RegisterConnectionDetailsFromURIFactory(connectionpb.ConnectionProvider_kafka,
		changefeedbase.SinkSchemeKafka, parseAndValidateKafkaSinkURI, makeKafkaSinkConnectionDetails)
}
