// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// buildKafkaTopicNamer creates a TopicNamer that mirrors the naming rules used by
// the sink. It consumes the topic-related query parameters (prefix,
// single-topic override) from the provided sinkURL so that later validation
// against RemainingQueryParams does not flag them as unknown.
func buildKafkaTopicNamer(
	targets changefeedbase.Targets, sinkURL *changefeedbase.SinkURL,
) (*TopicNamer, error) {
	prefix := sinkURL.ConsumeParam(changefeedbase.SinkParamTopicPrefix)
	singleName := sinkURL.ConsumeParam(changefeedbase.SinkParamTopicName)

	return MakeTopicNamer(
		targets,
		WithPrefix(prefix),
		WithSingleName(singleName),
		WithSanitizeFn(changefeedbase.SQLNameToKafkaName),
	)
}

// buildKafkaClients constructs a franz-go *kgo.Client along with a matching
// kadm admin client.  It is shared between the sink implementation and other
// changefeed code paths that need ad-hoc admin access (e.g. automatic topic
// creation during job startup).
//
// Callers own the returned client and should Close() it when finished; closing
// the *kgo.Client automatically tears down the admin client because the latter
// is just a lightweight wrapper.
func buildKafkaClients(
	ctx context.Context,
	bootstrapAddrs []string,
	allowAutoTopic bool,
	dialOpts []kgo.Opt,
	knobs kafkaSinkV2Knobs,
) (KafkaClientV2, KafkaAdminClientV2, error) {
	// Base opts mirror those used in the v2 sink but are centralised here so
	// that client construction is uniform across all changefeed components.
	baseOpts := []kgo.Opt{
		kgo.DisableIdempotentWrite(),
		kgo.SeedBrokers(bootstrapAddrs...),
		kgo.WithLogger(kgoLogAdapter{ctx: ctx}),
		kgo.RecordPartitioner(newKgoChangefeedPartitioner()),
		// 256MiB producer batch and 1GiB write—same as v2 sink.
		kgo.ProducerBatchMaxBytes(256 << 20),
		kgo.BrokerMaxWriteBytes(1 << 30),
		kgo.RecordRetries(5),
		kgo.RequestRetries(5),
		kgo.ProducerOnDataLossDetected(func(topic string, part int32) {
			log.Changefeed.Errorf(ctx, "kafka sink detected data loss for topic %s partition %d", redact.SafeString(topic), redact.SafeInt(part))
		}),
	}
	if allowAutoTopic {
		baseOpts = append(baseOpts, kgo.AllowAutoTopicCreation())
	}

	clientOpts := append(baseOpts, dialOpts...)

	if knobs.OverrideClient != nil {
		c, a := knobs.OverrideClient(clientOpts)
		if c == nil || a == nil {
			return nil, nil, errors.New("override client returned nil kafka client or admin client")
		}
		return c, a, nil
	}

	cli, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return nil, nil, err
	}
	return cli, kadm.NewClient(cli), nil
}
