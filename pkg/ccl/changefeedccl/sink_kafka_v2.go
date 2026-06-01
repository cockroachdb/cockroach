// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"hash/fnv"
	"io"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/cidr"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/klauspost/compress/zstd"
	"github.com/rcrowley/go-metrics"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

type kafkaSinkClientV2 struct {
	batchCfg    sinkBatchConfig
	client      KafkaClientV2
	adminClient KafkaAdminClientV2

	knobs               kafkaSinkV2Knobs
	canTryResizing      bool
	includeErrorDetails bool
	recordResize        func(numRecords int64)

	topicsForConnectionCheck []string
	constHeaders             []kgo.RecordHeader

	// we need to fetch and keep track of this ourselves since kgo doesnt expose metadata to us
	metadataMu struct {
		syncutil.Mutex
		allTopicPartitions  map[string][]int32
		lastMetadataRefresh time.Time
	}
}

// newKafkaSinkClientV2 creates a new kafka sink client. It is a thin wrapper
// around the kgo client for use by the batching sink. It's not meant to be
// invoked on its own, but rather through makeKafkaSinkV2.
func newKafkaSinkClientV2(
	ctx context.Context,
	clientOpts []kgo.Opt,
	batchCfg sinkBatchConfig,
	bootstrapAddrsStr string,
	settings *cluster.Settings,
	knobs kafkaSinkV2Knobs,
	mb metricsRecorderBuilder,
	topicsForConnectionCheck []string,
	constHeaders map[string][]byte,
	partitionAlg string,
	createTopics changefeedbase.CreateKafkaTopics,
) (*kafkaSinkClientV2, error) {
	bootstrapBrokers := strings.Split(bootstrapAddrsStr, `,`)

	baseOpts := []kgo.Opt{
		// Disable idempotency to maintain parity with the v1 sink and not add surface area for unknowns.
		kgo.DisableIdempotentWrite(),
		// Allow up to 5 concurrent produce requests per broker, matching the
		// Sarama client's default. This is safe because ParallelIO handles
		// ordering independently of franz-go.
		kgo.MaxProduceRequestsInflightPerBroker(5),

		kgo.SeedBrokers(bootstrapBrokers...),
		kgo.WithLogger(kgoLogAdapter{ctx: ctx}),
		kgo.RecordPartitioner(newKgoChangefeedPartitioner(partitionAlg)),
		kgo.ProducerBatchMaxBytes(int32(changefeedbase.KafkaMaxRequestSize.Get(&settings.SV))),
		kgo.BrokerMaxWriteBytes(1 << 30), // 1GiB

		kgo.RecordRetries(5),
		// This applies only to non-produce requests, ie the ListTopics call.
		kgo.RequestRetries(5),

		// This detects unavoidable data loss due to kafka cluster issues, and we may as well log it if it happens.
		// See #127246 for further work we can do here.
		kgo.ProducerOnDataLossDetected(func(topic string, part int32) {
			log.Changefeed.Errorf(ctx, `kafka sink detected data loss for topic %s partition %d`, redact.SafeString(topic), redact.SafeInt(part))
		}),
	}
	if createTopics == changefeedbase.CreateKafkaTopicsBrokerAuto {
		baseOpts = append(baseOpts, kgo.AllowAutoTopicCreation())
	}

	recordResize := func(numRecords int64) {}
	if m := mb(requiresResourceAccounting); m != nil { // `m` can be nil in tests.
		baseOpts = append(baseOpts, kgo.WithHooks(&kgoMetricsAdapter{throttling: m.getKafkaThrottlingMetrics(settings)}))
		recordResize = func(numRecords int64) {
			m.recordInternalRetry(numRecords, true)
		}
	}

	clientOpts = append(baseOpts, clientOpts...)

	var client KafkaClientV2
	var adminClient KafkaAdminClientV2
	var err error

	if knobs.OverrideClient != nil {
		client, adminClient = knobs.OverrideClient(clientOpts)
		if client == nil || adminClient == nil {
			return nil, errors.New("override client returned nil kafka client or admin client")
		}
	} else {
		client, err = kgo.NewClient(clientOpts...)
		if err != nil {
			return nil, err
		}
		adminClient = kadm.NewClient(client.(*kgo.Client))
	}

	constHeadersKgo := make([]kgo.RecordHeader, 0, len(constHeaders))
	for k, v := range constHeaders {
		constHeadersKgo = append(constHeadersKgo, kgo.RecordHeader{Key: k, Value: v})
	}

	c := &kafkaSinkClientV2{
		client:                   client,
		adminClient:              adminClient,
		knobs:                    knobs,
		batchCfg:                 batchCfg,
		canTryResizing:           changefeedbase.BatchReductionRetryEnabled.Get(&settings.SV),
		includeErrorDetails:      changefeedbase.KafkaV2ErrorDetailsEnabled.Get(&settings.SV),
		recordResize:             recordResize,
		topicsForConnectionCheck: topicsForConnectionCheck,
		constHeaders:             constHeadersKgo,
	}
	c.metadataMu.allTopicPartitions = make(map[string][]int32)

	return c, nil
}

// Close implements SinkClient.
func (k *kafkaSinkClientV2) Close() error {
	k.client.Close()
	return nil
}

// Flush implements SinkClient. Does not retry -- retries will be handled either by kafka or ParallelIO.
func (k *kafkaSinkClientV2) Flush(ctx context.Context, payload SinkPayload) (retErr error) {
	msgs := payload.([]*kgo.Record)

	var flushMsgs func(msgs []*kgo.Record) error
	flushMsgs = func(msgs []*kgo.Record) error {
		if err := k.client.ProduceSync(ctx, msgs...).FirstErr(); err != nil {
			if k.shouldTryResizing(err, msgs) {
				a, b := msgs[0:len(msgs)/2], msgs[len(msgs)/2:]
				log.Changefeed.Infof(ctx,
					"resizing kafka batch from %d messages to halves of %d and %d due to error: %v",
					redact.SafeInt(int64(len(msgs))),
					redact.SafeInt(int64(len(a))),
					redact.SafeInt(int64(len(b))),
					err)
				// Recurse. This is a little odd because the client's batch
				// state doesn't consist only of this payload, but the inflight
				// payloads of all ParallelIO workers. Therefore reducing the
				// size of this "batch" does not necessarily 1-1 cause a
				// reduction in the size of the actual batch the client sends.
				// Still, it should probably help.
				// Ideally users would set kafka-side max bytes appropriately
				// with respect to their average message sizes.
				k.recordResize(int64(len(a)))
				if err := flushMsgs(a); err != nil {
					return err
				}
				k.recordResize(int64(len(b)))
				if err := flushMsgs(b); err != nil {
					return err
				}
				return nil
			} else {
				if len(msgs) == 1 && errors.Is(err, kerr.MessageTooLarge) && k.includeErrorDetails {
					msg := msgs[0]
					mvccVal := msg.Context.Value(mvccTSKey{})
					var ts hlc.Timestamp
					if mvccVal != nil {
						ts = mvccVal.(hlc.Timestamp)
					}
					err = errors.Wrapf(err,
						"Kafka message too large: key=%s size=%d mvcc=%s",
						string(msg.Key), len(msg.Key)+len(msg.Value), ts,
					)
				}
				return err
			}
		}
		return nil
	}
	return flushMsgs(msgs)
}

// FlushResolvedPayload implements SinkClient.
func (k *kafkaSinkClientV2) FlushResolvedPayload(
	ctx context.Context,
	body []byte,
	forEachTopic func(func(topic string) error) error,
	retryOpts retry.Options,
) error {
	return retryOpts.Do(ctx, func(ctx context.Context) error {
		if err := k.maybeUpdateTopicPartitions(ctx, forEachTopic); err != nil {
			return err
		}
		// Re-lock the partitions out of an abundance of caution. We don't want to hold the lock during the Flush call, though.
		var msgs []*kgo.Record
		err := func() error {
			k.metadataMu.Lock()
			defer k.metadataMu.Unlock()
			msgs = make([]*kgo.Record, 0, len(k.metadataMu.allTopicPartitions))
			return forEachTopic(func(topic string) error {
				if _, ok := k.metadataMu.allTopicPartitions[topic]; !ok {
					log.Changefeed.Warningf(ctx, `cannot flush resolved timestamp for unknown topic %s`, topic)
				}
				for _, partition := range k.metadataMu.allTopicPartitions[topic] {
					msgs = append(msgs, &kgo.Record{
						Topic:     topic,
						Partition: partition,
						Key:       nil,
						Value:     body,
					})
				}
				return nil
			})
		}()
		if err != nil {
			return err
		}
		return k.Flush(ctx, msgs)
	})
}

func (k *kafkaSinkClientV2) CheckConnection(ctx context.Context) error {
	return k.maybeUpdateTopicPartitions(ctx, func(cb func(topic string) error) error {
		for _, topic := range k.topicsForConnectionCheck {
			if err := cb(topic); err != nil {
				return err
			}
		}
		return nil
	})
}

func (k *kafkaSinkClientV2) maybeUpdateTopicPartitions(
	ctx context.Context, forEachTopic func(func(topic string) error) error,
) error {
	k.metadataMu.Lock()
	defer k.metadataMu.Unlock()

	// Build list of topics.
	var topics []string
	if err := forEachTopic(func(topic string) error {
		topics = append(topics, topic)
		return nil
	}); err != nil {
		return err
	}

	// If we were asked to update metadata for new topics, always so. Otherwise check the time since last update.
	const metadataRefreshMinDuration = time.Minute
	if len(topics) == len(k.metadataMu.allTopicPartitions) && timeutil.Since(k.metadataMu.lastMetadataRefresh) < metadataRefreshMinDuration {
		return nil
	}

	log.Changefeed.Infof(ctx, `updating kafka metadata for topics: %+v`, topics)

	topicDetails, err := k.adminClient.ListTopics(ctx, topics...)
	if err != nil {
		return err
	}
	k.metadataMu.allTopicPartitions = make(map[string][]int32, len(topicDetails.TopicsList()))
	for _, td := range topicDetails.TopicsList() {
		k.metadataMu.allTopicPartitions[td.Topic] = td.Partitions
	}
	k.metadataMu.lastMetadataRefresh = timeutil.Now()

	return nil
}

// MakeBatchBuffer implements SinkClient.
func (k *kafkaSinkClientV2) MakeBatchBuffer(topic string) BatchBuffer {
	return &kafkaBuffer{topic: topic, batchCfg: k.batchCfg, constHeaders: k.constHeaders, includeErrorDetails: k.includeErrorDetails}
}

func (k *kafkaSinkClientV2) shouldTryResizing(err error, msgs []*kgo.Record) bool {
	if !k.canTryResizing || err == nil || len(msgs) < 2 {
		return false
	}
	// NOTE: This is what the v1 sink checks for, but I'm not convinced it's right. kerr.RecordListTooLarge sounds more like what we want.
	// From the docs: `If you produce a record that is larger than n, the record is immediately failed with kerr.MessageTooLarge.`
	// TODO(#127379): investigate this
	return errors.Is(err, kerr.MessageTooLarge)
}

// KafkaClientV2 is a small interface restricting the functionality in *kgo.Client
type KafkaClientV2 interface {
	ProduceSync(ctx context.Context, msgs ...*kgo.Record) kgo.ProduceResults
	Close()
}

// KafkaAdminClientV2 is a small interface restricting the functionality in
// *kadm.Client. It's used to list topics so we can iterate over all partitions
// to flush resolved messages.
type KafkaAdminClientV2 interface {
	ListTopics(ctx context.Context, topics ...string) (kadm.TopicDetails, error)
	ApiVersions(ctx context.Context) (kadm.BrokersApiVersions, error)
	ValidateCreateTopics(ctx context.Context, partitions int32, replicationFactor int16, configs map[string]*string, topics ...string) (kadm.CreateTopicResponses, error)
	CreateTopics(ctx context.Context, partitions int32, replicationFactor int16, configs map[string]*string, topics ...string) (kadm.CreateTopicResponses, error)
}

type kafkaSinkV2Knobs struct {
	OverrideClient              func(opts []kgo.Opt) (KafkaClientV2, KafkaAdminClientV2)
	SkipCreateTopicVersionCheck bool
}

var _ SinkClient = (*kafkaSinkClientV2)(nil)
var _ SinkPayload = ([]*kgo.Record)(nil) // NOTE: This doesn't actually assert anything, but it's good documentation.

type kafkaBuffer struct {
	topic     string
	messages  []*kgo.Record
	byteCount int

	batchCfg            sinkBatchConfig
	constHeaders        []kgo.RecordHeader
	includeErrorDetails bool
}

type mvccTSKey struct{}

func (b *kafkaBuffer) Append(ctx context.Context, key []byte, value []byte, attrs attributes) {
	// HACK: kafka sink v1 encodes nil keys as sarama.ByteEncoder(key) which is != nil, and unit tests rely on this.
	// So do something equivalent.
	if key == nil {
		key = []byte{}
	}

	headers := make([]kgo.RecordHeader, 0, len(b.constHeaders)+len(attrs.headers))
	headers = append(headers, b.constHeaders...)

	for k, v := range attrs.headers {
		headers = append(headers, kgo.RecordHeader{Key: k, Value: v})
	}

	var rctx context.Context
	if b.includeErrorDetails {
		rctx = context.WithValue(ctx, mvccTSKey{}, attrs.mvcc)
	}

	b.messages = append(b.messages, &kgo.Record{Key: key, Value: value, Topic: b.topic, Headers: headers, Context: rctx})
	b.byteCount += len(value)
}

func (b *kafkaBuffer) Close() (SinkPayload, error) {
	return b.messages, nil
}

func (b *kafkaBuffer) ShouldFlush() bool {
	return shouldFlushBatch(b.byteCount, len(b.messages), b.batchCfg)
}

var _ BatchBuffer = (*kafkaBuffer)(nil)

func makeKafkaSinkV2(
	ctx context.Context,
	u *changefeedbase.SinkURL,
	targets changefeedbase.Targets,
	sinkOpts changefeedbase.KafkaSinkOptions,
	parallelism int,
	pacerFactory func() *admission.Pacer,
	timeSource timeutil.TimeSource,
	settings *cluster.Settings,
	mb metricsRecorderBuilder,
	knobs kafkaSinkV2Knobs,
	createTopics changefeedbase.CreateKafkaTopics,
) (Sink, error) {
	jsonConfig := sinkOpts.JSONConfig
	batchCfg, retryOpts, err := getSinkConfigFromJson(jsonConfig, sinkJSONConfig{
		// Defaults from the v1 sink - flush immediately.
		Flush: sinkBatchConfig{},
		// These are sarama's defaults.
		Retry: sinkRetryConfig{
			Max:     3,
			Backoff: jsonDuration(100 * time.Millisecond),
		},
	})
	if err != nil {
		return nil, err
	}

	kafkaTopicPrefix := u.ConsumeParam(changefeedbase.SinkParamTopicPrefix)
	kafkaTopicName := u.ConsumeParam(changefeedbase.SinkParamTopicName)
	if schemaTopic := u.ConsumeParam(changefeedbase.SinkParamSchemaTopic); schemaTopic != `` {
		return nil, errors.Errorf(`%s is not yet supported`, changefeedbase.SinkParamSchemaTopic)
	}

	clientOpts, err := buildKgoConfig(ctx, u, jsonConfig, mb(true).netMetrics())
	if err != nil {
		return nil, err
	}

	topicNamer, err := MakeTopicNamer(
		targets,
		WithPrefix(kafkaTopicPrefix), WithSingleName(kafkaTopicName), WithSanitizeFn(changefeedbase.SQLNameToKafkaName))

	if err != nil {
		return nil, err
	}

	if unknownParams := u.RemainingQueryParams(); len(unknownParams) > 0 {
		return nil, errors.Errorf(
			`unknown kafka sink query parameters: %s`, strings.Join(unknownParams, ", "))
	}

	topicsForConnectionCheck := topicNamer.DisplayNamesSlice()
	client, err := newKafkaSinkClientV2(
		ctx, clientOpts, batchCfg, u.Host, settings, knobs, mb, topicsForConnectionCheck,
		sinkOpts.Headers, sinkOpts.PartitionAlg, createTopics,
	)
	if err != nil {
		return nil, err
	}

	return makeBatchingSink(ctx, sinkTypeKafka, client, time.Duration(batchCfg.Frequency), retryOpts,
		parallelism, topicNamer, pacerFactory, timeSource, mb(true), settings), nil
}

// maybeCreateKafkaTopics creates any missing kafka topics for the changefeed
// when create_kafka_topics=explicit. No-op for other modes or non-kafka sinks;
// errors if the v2 kafka sink is disabled (only path that implements this).
func maybeCreateKafkaTopics(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	details jobspb.ChangefeedDetails,
	targets changefeedbase.Targets,
	schemaTS hlc.Timestamp,
	knobs kafkaSinkV2Knobs,
) error {
	ctx, span := tracing.ChildSpan(ctx, "changefeed.create_kafka_topics")
	defer span.Finish()

	opts := changefeedbase.MakeStatementOptions(details.Opts)
	createTopics, err := opts.GetCreateKafkaTopics()
	if err != nil {
		return err
	}
	if createTopics != changefeedbase.CreateKafkaTopicsExplicit {
		return nil
	}

	resolvedSinkURI, err := resolveDest(ctx, execCfg, details.SinkURI)
	if err != nil {
		return err
	}
	parsedSinkURL, err := url.Parse(resolvedSinkURI)
	if err != nil {
		return err
	}
	if !isKafkaSink(parsedSinkURL) {
		return nil
	}
	if !KafkaV2Enabled.Get(&execCfg.Settings.SV) {
		return errors.Newf("%s=explicit requires the v2 kafka sink; enable it with the %q cluster setting",
			changefeedbase.OptCreateKafkaTopics, KafkaV2Enabled.Name())
	}
	sinkURL := &changefeedbase.SinkURL{URL: parsedSinkURL}

	topics, err := kafkaTopicNamesForTargets(ctx, execCfg, targets, sinkURL, schemaTS)
	if err != nil {
		return err
	}
	if len(topics) == 0 {
		return nil
	}

	sinkOpts, err := opts.GetKafkaSinkOptions()
	if err != nil {
		return err
	}
	client, adminClient, err := newKafkaAdminClientV2(ctx, sinkURL, sinkOpts, knobs)
	if err != nil {
		return err
	}
	defer client.Close()

	return createMissingKafkaTopics(ctx, adminClient, topics, knobs)
}

// kafkaTopicNamesForTargets returns the deduplicated, sanitized topic names
// this changefeed will emit to.
func kafkaTopicNamesForTargets(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	targets changefeedbase.Targets,
	sinkURL *changefeedbase.SinkURL,
	schemaTS hlc.Timestamp,
) ([]string, error) {
	prefix := sinkURL.ConsumeParam(changefeedbase.SinkParamTopicPrefix)
	singleName := sinkURL.ConsumeParam(changefeedbase.SinkParamTopicName)
	return ResolveTopicNames(
		targets,
		func() ([]catalog.TableDescriptor, error) {
			return fetchTableDescriptors(ctx, execCfg, targets, schemaTS)
		},
		WithPrefix(prefix),
		WithSingleName(singleName),
		WithSanitizeFn(changefeedbase.SQLNameToKafkaName),
	)
}

func newKafkaAdminClientV2(
	ctx context.Context,
	u *changefeedbase.SinkURL,
	sinkOpts changefeedbase.KafkaSinkOptions,
	knobs kafkaSinkV2Knobs,
) (KafkaClientV2, KafkaAdminClientV2, error) {
	bootstrapBrokers := strings.Split(u.Host, `,`)
	clientOpts, err := buildKgoConfig(ctx, u, sinkOpts.JSONConfig, nil /* netMetrics */)
	if err != nil {
		return nil, nil, err
	}
	baseOpts := []kgo.Opt{

		kgo.DisableIdempotentWrite(),
		kgo.SeedBrokers(bootstrapBrokers...),
		kgo.ProducerBatchMaxBytes(256 << 20),
		kgo.BrokerMaxWriteBytes(1 << 30),
		kgo.RecordRetries(5),
		kgo.WithLogger(kgoLogAdapter{ctx: ctx}),
		kgo.RequestRetries(5),
	}
	clientOpts = append(baseOpts, clientOpts...)

	if knobs.OverrideClient != nil {
		client, adminClient := knobs.OverrideClient(clientOpts)
		if client == nil || adminClient == nil {
			return nil, nil, errors.New("override client returned nil kafka client or admin client")
		}
		return client, adminClient, nil
	}

	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return nil, nil, err
	}
	return client, kadm.NewClient(client), nil
}

// createMissingKafkaTopics creates topics not already on the broker, using
// broker-side defaults for partitions/replication.
func createMissingKafkaTopics(
	ctx context.Context, adminClient KafkaAdminClientV2, topics []string, knobs kafkaSinkV2Knobs,
) error {
	missing, err := missingKafkaTopics(ctx, adminClient, topics)
	if err != nil {
		return err
	}
	if len(missing) == 0 {
		log.Changefeed.VInfof(ctx, 2, "all kafka topics already exist")
		return nil
	}
	if err := ensureCreateTopicsDefaultsSupported(ctx, adminClient, knobs); err != nil {
		return err
	}

	validateResp, err := adminClient.ValidateCreateTopics(ctx, -1, -1, nil, missing...)
	if err != nil {
		return err
	}
	for _, topic := range missing {
		if resp, ok := validateResp[topic]; ok && resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
			return errors.Wrapf(resp.Err, "failed to validate kafka topic creation for topic %s", topic)
		}
	}

	log.Changefeed.Infof(ctx, "creating missing kafka topics: %v", missing)
	createResp, err := adminClient.CreateTopics(ctx, -1, -1, nil, missing...)
	if err != nil {
		return err
	}
	for _, topic := range missing {
		resp, ok := createResp[topic]
		if !ok {
			continue
		}
		if resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
			return errors.Wrapf(resp.Err, "failed to create kafka topic %s", topic)
		}
		log.Changefeed.Infof(ctx, "created kafka topic %s with num_partitions=%d replication_factor=%d",
			topic, resp.NumPartitions, resp.ReplicationFactor)
	}
	return nil
}

// missingKafkaTopics returns the subset of topics not present on the broker.
func missingKafkaTopics(
	ctx context.Context, adminClient KafkaAdminClientV2, topics []string,
) ([]string, error) {
	topicDetails, err := adminClient.ListTopics(ctx, topics...)
	if err != nil {
		return nil, err
	}

	missing := make([]string, 0, len(topics))
	for _, topic := range topics {
		details, ok := topicDetails[topic]
		if ok && details.Err == nil {
			continue
		}
		if ok && !errors.Is(details.Err, kerr.UnknownTopicOrPartition) {
			return nil, errors.Wrapf(details.Err, "failed to list kafka topic %s", topic)
		}
		missing = append(missing, topic)
	}
	return missing, nil
}

// ensureCreateTopicsDefaultsSupported errors if any broker is too old. That
// behavior requires CreateTopics v4 (Kafka 2.4+).
func ensureCreateTopicsDefaultsSupported(
	ctx context.Context, adminClient KafkaAdminClientV2, knobs kafkaSinkV2Knobs,
) error {
	if knobs.SkipCreateTopicVersionCheck {
		return nil
	}

	v24 := kversion.V2_4_0()
	createTopicsVersion, ok := v24.LookupMaxKeyVersion(kmsg.CreateTopics.Int16())
	if !ok {
		return errors.AssertionFailedf("v2.4.0 does not support create topics but it should")
	}

	versions, err := adminClient.ApiVersions(ctx)
	if err != nil {
		return err
	}
	for brokerID, version := range versions {
		if version.Err != nil {
			return errors.Wrapf(version.Err, "failed to get kafka api versions for broker %d", brokerID)
		}
		maxVersion, ok := version.KeyMaxVersion(kmsg.CreateTopics.Int16())
		if !ok {
			return errors.Errorf("kafka broker %d does not support CreateTopics", brokerID)
		}
		if maxVersion < createTopicsVersion {
			return errors.Errorf("kafka broker %d does not support CreateTopics defaults; broker max version %d is lower than required version %d",
				brokerID, maxVersion, createTopicsVersion)
		}
	}
	return nil
}

func buildKgoConfig(
	ctx context.Context,
	u *changefeedbase.SinkURL,
	jsonStr changefeedbase.SinkSpecificJSONConfig,
	netMetrics *cidr.NetMetrics,
) ([]kgo.Opt, error) {
	var opts []kgo.Opt

	dialConfig, err := buildDialConfig(u)
	if err != nil {
		return nil, err
	}

	if dialConfig.tlsEnabled {
		tlsCfg := &tls.Config{InsecureSkipVerify: dialConfig.tlsSkipVerify}
		if dialConfig.caCert != nil {
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(dialConfig.caCert)
			tlsCfg.RootCAs = caCertPool
		}

		if dialConfig.clientCert != nil && dialConfig.clientKey == nil {
			return nil, errors.Errorf(`%s requires %s to be set`, changefeedbase.SinkParamClientCert, changefeedbase.SinkParamClientKey)
		} else if dialConfig.clientKey != nil && dialConfig.clientCert == nil {
			return nil, errors.Errorf(`%s requires %s to be set`, changefeedbase.SinkParamClientKey, changefeedbase.SinkParamClientCert)
		}

		if dialConfig.clientCert != nil && dialConfig.clientKey != nil {
			cert, err := tls.X509KeyPair(dialConfig.clientCert, dialConfig.clientKey)
			if err != nil {
				return nil, errors.Wrap(err, `invalid client certificate data provided`)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}
		// The 10s dial timeout is the default in kgo if you don't manually
		// specify a Dialer. Since we are creating one we want to match the
		// default behavior. See kgo.NewClient.
		dialer := &net.Dialer{Timeout: 10 * time.Second}
		tlsDialer := &tls.Dialer{NetDialer: dialer, Config: tlsCfg}
		opts = append(opts, kgo.Dialer(netMetrics.Wrap(tlsDialer.DialContext, "kafka")))
	} else {
		if dialConfig.caCert != nil {
			return nil, errors.Errorf(`%s requires %s=true`, changefeedbase.SinkParamCACert, changefeedbase.SinkParamTLSEnabled)
		}
		if dialConfig.clientCert != nil {
			return nil, errors.Errorf(`%s requires %s=true`, changefeedbase.SinkParamClientCert, changefeedbase.SinkParamTLSEnabled)
		}
		// The 10s dial timeout is the default in kgo if you don't manually
		// specify a Dialer. Since we are creating one we want to match the
		// default behavior. See kgo.NewClient.
		dialer := &net.Dialer{Timeout: 10 * time.Second}
		opts = append(opts, kgo.Dialer(netMetrics.Wrap(dialer.DialContext, "kafka")))
	}

	if dialConfig.authMechanism != nil {
		authOpts, err := dialConfig.authMechanism.KgoOpts(ctx)
		if err != nil {
			return nil, err
		}
		opts = append(opts, authOpts...)
		log.Changefeed.VInfof(ctx, 2, "applied kafka auth mechanism: %+#v\n", dialConfig.authMechanism)
	}

	// Apply some statement level overrides. The flush related ones (Messages, MaxMessages, Bytes) are not applied here, but on the sinkBatchConfig instead.
	// TODO(#126991): Remove this sarama dependency.
	sinkCfg, err := getSaramaConfig(jsonStr)
	if err != nil {
		return nil, errors.Wrapf(err,
			"failed to parse sink config; check %s option", changefeedbase.OptKafkaSinkConfig)
	}
	if sinkCfg.Flush.MaxBytes != 0 {
		if sinkCfg.Flush.MaxBytes < changefeedbase.KafkaMaxRequestSizeMin {
			return nil, errors.Errorf(
				"MaxBytes must be at least %d bytes, got %d",
				changefeedbase.KafkaMaxRequestSizeMin, sinkCfg.Flush.MaxBytes)
		}
		if sinkCfg.Flush.MaxBytes > changefeedbase.KafkaMaxRequestSizeLimit {
			return nil, errors.Errorf(
				"MaxBytes must be at most %d bytes, got %d",
				changefeedbase.KafkaMaxRequestSizeLimit, sinkCfg.Flush.MaxBytes)
		}
		// Override the cluster setting default set in baseOpts; kgo uses last-writer-wins.
		opts = append(opts, kgo.ProducerBatchMaxBytes(sinkCfg.Flush.MaxBytes))
	}

	// If the user sets MaxMessages, use that as kgo's overall max batch size.
	// This can reduce our throughput due to ParallelIO's parallism, but we need
	// to respect these settings in case they're being specified to line up with
	// kafka cluster settings. We don't provide similar guarantees to
	// Flush.Bytes, so we don't need to respect that here. NOTE: this is set to
	// 1k by default, which is probably not great.
	if sinkCfg.Flush.MaxMessages > 0 {
		opts = append(opts, kgo.MaxBufferedRecords(sinkCfg.Flush.MaxMessages))
	}

	if sinkCfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(sinkCfg.ClientID))
	}

	switch strings.ToUpper(sinkCfg.RequiredAcks) {
	case ``, `ONE`, `1`: // This is our default.
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
	case `ALL`, `-1`:
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	case `NONE`, `0`:
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
	default:
		return nil, errors.Errorf(`unknown required acks value: %s`, sinkCfg.RequiredAcks)
	}

	// TODO(#126991): Remove this sarama dependency.
	// NOTE: kgo lets you give multiple compression options in preference order, which is cool but the config json doesnt support that. Should we?
	var comp kgo.CompressionCodec
	switch sarama.CompressionCodec(sinkCfg.Compression) {
	case sarama.CompressionNone:
	case sarama.CompressionGZIP:
		comp = kgo.GzipCompression()
	case sarama.CompressionSnappy:
		comp = kgo.SnappyCompression()
	case sarama.CompressionLZ4:
		comp = kgo.Lz4Compression()
	case sarama.CompressionZSTD:
		comp = kgo.ZstdCompression()
	default:
		return nil, errors.Errorf(`unknown compression codec: %v`, sinkCfg.Compression)
	}

	if level := sinkCfg.CompressionLevel; level != sarama.CompressionLevelDefault {
		if err := validateCompressionLevel(sinkCfg.Compression, level); err != nil {
			return nil, err
		}
		comp = comp.WithLevel(level)
	}

	opts = append(opts, kgo.ProducerBatchCompression(comp))

	if version := sinkCfg.Version; version != "" {
		if !strings.HasPrefix(version, `v`) {
			version = `v` + version
		}
		v := kversion.FromString(version)
		if v == nil {
			return nil, errors.Errorf(`unknown kafka version: %s`, version)
		}
		// NOTE: This version of kgo doesn't support specifying max versions
		// >3.6.0 (released Oct 10 2023). This option is only really needed for
		// interacting with <v0.10.0 clusters anyway.
		opts = append(opts, kgo.MaxVersions(v))
	}

	return opts, nil
}

// NOTE: kgo will ignore invalid compression levels, but the v1 sinks will fail validations. So we have to validate these ourselves.
func validateCompressionLevel(compressionType compressionCodec, level int) error {
	switch sarama.CompressionCodec(compressionType) {
	case sarama.CompressionNone:
		return nil
	case sarama.CompressionGZIP:
		if level < gzip.HuffmanOnly || level > gzip.BestCompression {
			return errors.Errorf(`invalid gzip compression level: %d`, level)
		}
	case sarama.CompressionSnappy:
		return errors.Errorf(`snappy does not support compression levels`)
	case sarama.CompressionLZ4:
		// The v1 sink ignores `level` for lz4, So let's use kgo's default
		// behavior, which is to apply the level if it's valid, and fall back to
		// the default otherwise.
		return nil
	case sarama.CompressionZSTD:
		w, err := zstd.NewWriter(io.Discard, zstd.WithEncoderLevel(zstd.EncoderLevel(level)))
		if err != nil {
			return errors.Errorf(`invalid zstd compression level: %d`, level)
		}
		_ = w.Close()
	default:
		return errors.Errorf(`unknown compression codec: %v`, compressionType)
	}
	return nil
}

type kgoLogAdapter struct {
	ctx context.Context
}

func (k kgoLogAdapter) Level() kgo.LogLevel {
	if log.V(2) {
		return kgo.LogLevelDebug
	}
	return kgo.LogLevelInfo
}

func (k kgoLogAdapter) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	format := `kafka(kgo): %s %s`
	for i := 0; i < len(keyvals); i += 2 {
		format += ` %s=%v`
	}
	log.Changefeed.InfofDepth(k.ctx, 1, format, append([]any{redact.SafeString(level.String()), redact.SafeString(msg)}, keyvals...)...) //nolint:fmtsafe
}

var _ kgo.Logger = kgoLogAdapter{}

// newKgoChangefeedPartitioner returns a new kgo.Partitioner that wraps the
// recommended sarama compat approach to pass thru record partitions when key is
// nil, like the v1 implementation.
// hashMethod specifies the hash function to use: "fnv-1a" (default) or "murmur2".
func newKgoChangefeedPartitioner(hashMethod string) kgo.Partitioner {
	if strings.ToLower(hashMethod) == "murmur2" {
		// murmur2 is the franz-go default. StickyKeyPartitioner creates a
		// new murmur2 hasher every call. If the default were to change,
		// TestPartitionAlg should fail.
		inner := kgo.StickyKeyPartitioner(nil /* hasher */)
		return &kgoChangefeedPartitioner{inner: inner}
	}
	// Use fnv-1a by default. fnv-1a is the sarama default hashing
	// algorithm and is compatible with the v1 kafka sink.
	inner := kgo.StickyKeyPartitioner(kgo.SaramaCompatHasher(func(
		bs []byte) uint32 {
		hasher := fnv.New32a()
		_, _ = hasher.Write(bs)
		return hasher.Sum32()
	}))

	return &kgoChangefeedPartitioner{inner: inner}
}

type kgoChangefeedPartitioner struct {
	inner kgo.Partitioner
}

func (p *kgoChangefeedPartitioner) ForTopic(topic string) kgo.TopicPartitioner {
	return &kgoChangefeedTopicPartitioner{inner: p.inner.ForTopic(topic)}
}

type kgoChangefeedTopicPartitioner struct {
	inner kgo.TopicPartitioner
}

func (p *kgoChangefeedTopicPartitioner) RequiresConsistency(*kgo.Record) bool { return true }
func (p *kgoChangefeedTopicPartitioner) Partition(r *kgo.Record, n int) int {
	// Let messages without keys specify where they want to go (for resolved messages).
	// Note that while CSV rows don't have keys, we set set the
	// kgo.Record.Key to []byte{} in BatchBuffer.Append, so this will still only
	// affect resolved messages.
	if r.Key == nil {
		return int(r.Partition)
	}
	return p.inner.Partition(r, n)
}

type kgoMetricsAdapter struct {
	throttling metrics.Histogram
}

func (k *kgoMetricsAdapter) OnBrokerThrottle(
	meta kgo.BrokerMetadata, throttleInterval time.Duration, throttledAfterResponse bool,
) {
	// There's an interceptor in here that assumes the histogram value is in
	// milliseconds instead of nanoseconds, so we need to convert.
	k.throttling.Update(int64(throttleInterval / time.Millisecond))
}

var _ kgo.HookBrokerThrottle = (*kgoMetricsAdapter)(nil)
