package changefeedccl

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"hash/fnv"
	"net/url"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/rcrowley/go-metrics"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl"
	sasloauth "github.com/twmb/franz-go/pkg/sasl/oauth"
	saslplain "github.com/twmb/franz-go/pkg/sasl/plain"
	saslscram "github.com/twmb/franz-go/pkg/sasl/scram"
	"golang.org/x/oauth2/clientcredentials"
)

func newKafkaSinkClient(
	ctx context.Context,
	clientOpts []kgo.Opt,
	batchCfg sinkBatchConfig,
	bootstrapAddrs string,
	settings *cluster.Settings,
	knobs kafkaSinkV2Knobs,
) (*kafkaSinkClient, error) {
	clientOpts = append([]kgo.Opt{
		// TODO: hooks for metrics / metric support at all
		kgo.SeedBrokers(bootstrapAddrs),
		kgo.ClientID(`cockroach`),
		// kgo.DefaultProduceTopic(topic), // TODO: maybe, depending on if we end up sharing producers
		kgo.WithLogger(kgoLogAdapter{ctx: ctx}),
		kgo.RecordPartitioner(newKgoChangefeedPartitioner()),
		kgo.ProducerBatchMaxBytes(2 << 27), // nearly parity - this is the max the library allows
		kgo.BrokerMaxWriteBytes(2 << 27),   // have to bump this as well
		// idempotent production is strictly a win, but does require the IDEMPOTENT_WRITE permission on CLUSTER (pre Kafka 3.0), and not all clients can have that permission.
		// i think sarama also transparently enables this and we dont disable it there so we shouldnt need to here.. right?
		// also does this gracefully disable it or error if the permission is missing?
		// kgo.DisableIdempotentWrite(),

		// kgo.MaxProduceRequestsInflightPerBroker(1) // default is 1, or 5 if idempotent is enabled (it is by default)
		// kgo.ManualFlushing() ?

		// we do need to fail eventually.. right?
		// NOTE: If idempotency is enabled (as it is by default), this option is
		// only enforced if it is safe to do so without creating invalid
		// sequence numbers. It is safe to enforce if a record was never issued
		// in a request to Kafka, or if it was requested and received a
		// response.
		// kgo.RecordRetries(5),
		// TODO: test that produce will indeed fail eventually if it keeps getting errors
	}, clientOpts...)

	var client KafkaClientV2
	var adminClient KafkaAdminClientV2
	var err error
	if knobs.OverrideClient != nil {
		client, adminClient = knobs.OverrideClient(clientOpts)
	} else {
		client, err = kgo.NewClient(clientOpts...)
		if err != nil {
			return nil, err
		}
		adminClient = kadm.NewClient(client.(*kgo.Client))
	}

	c := &kafkaSinkClient{
		client:         client,
		adminClient:    adminClient,
		knobs:          knobs,
		batchCfg:       batchCfg,
		canTryResizing: changefeedbase.BatchReductionRetryEnabled.Get(&settings.SV),
	}
	c.metadataMu.allTopicPartitions = make(map[string][]int32)

	return c, nil
}

// TODO: rename, with v2 in there somewhere
type kafkaSinkClient struct {
	batchCfg    sinkBatchConfig
	client      KafkaClientV2
	adminClient KafkaAdminClientV2

	knobs          kafkaSinkV2Knobs
	canTryResizing bool

	// we need to fetch and keep track of this ourselves since kgo doesnt expose metadata to us
	metadataMu struct {
		syncutil.Mutex
		allTopicPartitions  map[string][]int32
		lastMetadataRefresh time.Time
	}

	debuggingId int64
}

// Close implements SinkClient.
func (k *kafkaSinkClient) Close() error {
	k.client.Close()
	return nil
}

// Flush implements SinkClient. Does not retry -- retries will be handled by ParallelIO.
func (k *kafkaSinkClient) Flush(ctx context.Context, payload SinkPayload) (retErr error) {
	msgs := payload.([]*kgo.Record)
	defer log.Infof(ctx, `flushed %d messages to kafka (id=%d, err=%v)`, len(msgs), k.debuggingId, retErr)

	// TODO: make this better. possibly moving the resizing up into the batch worker would help a bit
	var flushMsgs func(msgs []*kgo.Record) error
	flushMsgs = func(msgs []*kgo.Record) error {
		handleErr := func(err error) error {
			log.Infof(ctx, `kafka error in %d: %s`, k.debuggingId, err.Error())
			if k.shouldTryResizing(err, msgs) {
				a, b := msgs[0:len(msgs)/2], msgs[len(msgs)/2:]
				// recurse
				return errors.Join(flushMsgs(a), flushMsgs(b))
			}

			return err
		}
		if err := k.client.ProduceSync(ctx, msgs...).FirstErr(); err != nil {
			return handleErr(err)
		}
		return nil
	}
	return flushMsgs(msgs)
}

// FlushResolvedPayload implements SinkClient.
func (k *kafkaSinkClient) FlushResolvedPayload(
	ctx context.Context,
	body []byte,
	forEachTopic func(func(topic string) error) error,
	retryOpts retry.Options,
) error {
	k.metadataMu.Lock()
	defer k.metadataMu.Unlock()

	if err := k.maybeUpdateTopicPartitionsLocked(ctx, forEachTopic); err != nil {
		return err
	}
	msgs := make([]*kgo.Record, 0, len(k.metadataMu.allTopicPartitions))
	err := forEachTopic(func(topic string) error {
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
	if err != nil {
		return err
	}

	return k.Flush(ctx, msgs)
}

// update metadata ourselves since kgo doesnt expose it to us :/
func (k *kafkaSinkClient) maybeUpdateTopicPartitionsLocked(ctx context.Context, forEachTopic func(func(topic string) error) error) error {
	k.metadataMu.AssertHeld()
	log.Infof(ctx, `updating topic partitions for kafka sink`)

	const metadataRefreshMinDuration = time.Minute
	if timeutil.Now().Sub(k.metadataMu.lastMetadataRefresh) < metadataRefreshMinDuration {
		return nil
	}

	// Build list of topics.
	var topics []string
	if err := forEachTopic(func(topic string) error {
		topics = append(topics, topic)
		return nil
	}); err != nil {
		return err
	}

	topicDetails, err := k.adminClient.ListTopics(ctx, topics...)
	if err != nil {
		return err
	}
	for _, td := range topicDetails.TopicsList() {
		k.metadataMu.allTopicPartitions[td.Topic] = td.Partitions
	}

	return nil
}

// MakeBatchBuffer implements SinkClient.
func (k *kafkaSinkClient) MakeBatchBuffer(topic string) BatchBuffer {
	return &kafkaBuffer{topic: topic, batchCfg: k.batchCfg}
}

func (k *kafkaSinkClient) shouldTryResizing(err error, msgs []*kgo.Record) bool {
	if !k.canTryResizing || err == nil || len(msgs) < 2 {
		return false
	}
	var kError sarama.KError
	return errors.As(err, &kError) && kError == sarama.ErrMessageSizeTooLarge
}

// KafkaClientV2 is a small interface restricting the functionality in *kgo.Client
type KafkaClientV2 interface {
	// k.client.ProduceSync(ctx, msgs...).FirstErr(); err != nil {
	ProduceSync(ctx context.Context, msgs ...*kgo.Record) kgo.ProduceResults
	Close()
}

type KafkaAdminClientV2 interface {
	ListTopics(ctx context.Context, topics ...string) (kadm.TopicDetails, error)
}

type kafkaSinkV2Knobs struct {
	OverrideClient func(opts []kgo.Opt) (KafkaClientV2, KafkaAdminClientV2)
}

var _ SinkClient = (*kafkaSinkClient)(nil)
var _ SinkPayload = ([]*kgo.Record)(nil) // this doesnt actually assert anything fyi

type keyPlusPayload struct {
	key     []byte
	payload []byte
}

type kafkaBuffer struct {
	topic     string
	messages  []keyPlusPayload
	byteCount int

	batchCfg sinkBatchConfig
}

// Append implements BatchBuffer.
func (b *kafkaBuffer) Append(key []byte, value []byte, _ attributes) {
	b.messages = append(b.messages, keyPlusPayload{key: key, payload: value})
	b.byteCount += len(value)
}

// Close implements BatchBuffer. Convert the buffer into a SinkPayload for sending to kafka.
func (b *kafkaBuffer) Close() (SinkPayload, error) {
	msgs := make([]*kgo.Record, 0, len(b.messages))
	for _, m := range b.messages {
		msgs = append(msgs, &kgo.Record{
			Topic: b.topic,
			Key:   m.key,
			Value: m.payload,
			// can use Context field for tracking/metrics if desired
		})
	}
	return msgs, nil
}

// ShouldFlush implements BatchBuffer.
func (b *kafkaBuffer) ShouldFlush() bool {
	return shouldFlushBatch(b.byteCount, len(b.messages), b.batchCfg)
}

var _ BatchBuffer = (*kafkaBuffer)(nil)

func makeKafkaSinkV2(ctx context.Context,
	u sinkURL,
	targets changefeedbase.Targets,
	jsonConfig changefeedbase.SinkSpecificJSONConfig,
	parallelism int,
	pacerFactory func() *admission.Pacer,
	timeSource timeutil.TimeSource,
	settings *cluster.Settings,
	mb metricsRecorderBuilder,
) (Sink, error) {
	batchCfg, retryOpts, err := getSinkConfigFromJson(jsonConfig, sinkJSONConfig{
		// Defaults from the old kafka sink (nearly -- we require Frequency to be nonzero if anything else is, but the old sink did not. Set it low.)
		Flush: sinkBatchConfig{
			Frequency: jsonDuration(1 * time.Millisecond),
			Messages:  1000,
		},
		// Retry: {} ? TODO
	})
	if err != nil {
		return nil, err
	}

	kafkaTopicPrefix := u.consumeParam(changefeedbase.SinkParamTopicPrefix)
	kafkaTopicName := u.consumeParam(changefeedbase.SinkParamTopicName)
	if schemaTopic := u.consumeParam(changefeedbase.SinkParamSchemaTopic); schemaTopic != `` {
		return nil, errors.Errorf(`%s is not yet supported`, changefeedbase.SinkParamSchemaTopic)
	}

	m := mb(requiresResourceAccounting)
	clientOpts, err := buildKgoConfig(ctx, u, jsonConfig, m.getKafkaThrottlingMetrics(settings))
	if err != nil {
		return nil, err
	}

	topicNamer, err := MakeTopicNamer(
		targets,
		WithPrefix(kafkaTopicPrefix), WithSingleName(kafkaTopicName), WithSanitizeFn(SQLNameToKafkaName))

	if err != nil {
		return nil, err
	}

	if unknownParams := u.remainingQueryParams(); len(unknownParams) > 0 {
		return nil, errors.Errorf(
			`unknown kafka sink query parameters: %s`, strings.Join(unknownParams, ", "))
	}

	client, err := newKafkaSinkClient(ctx, clientOpts, batchCfg, u.Host, settings, kafkaSinkV2Knobs{})
	if err != nil {
		return nil, err
	}

	return makeBatchingSink(ctx, sinkTypeKafka, client, nil, time.Second, retryOpts,
		parallelism, topicNamer, pacerFactory, timeSource, mb(true), settings), nil
}

func buildKgoConfig(
	ctx context.Context,
	u sinkURL,
	jsonStr changefeedbase.SinkSpecificJSONConfig,
	kafkaThrottlingMetrics metrics.Histogram,
) ([]kgo.Opt, error) {
	// TODO: what's the equivalent of the frequency option? is there even one? like maybe not
	var opts []kgo.Opt

	dialConfig, err := buildDialConfig(u)
	if err != nil {
		return nil, err
	}

	// TODO: metrics
	// config.MetricRegistry = newMetricsRegistryInterceptor(kafkaThrottlingMetrics)

	if dialConfig.tlsEnabled {
		tlsCfg := &tls.Config{InsecureSkipVerify: dialConfig.tlsSkipVerify}
		if dialConfig.caCert != nil {
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(dialConfig.caCert)
			tlsCfg.RootCAs = caCertPool
		}

		// TODO: not sure why this validation is here and not in buildDialConfig()
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
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	} else {
		// ditto
		if dialConfig.caCert != nil {
			return nil, errors.Errorf(`%s requires %s=true`, changefeedbase.SinkParamCACert, changefeedbase.SinkParamTLSEnabled)
		}
		if dialConfig.clientCert != nil {
			return nil, errors.Errorf(`%s requires %s=true`, changefeedbase.SinkParamClientCert, changefeedbase.SinkParamTLSEnabled)
		}
	}

	if dialConfig.saslEnabled {
		var sasl sasl.Mechanism
		switch dialConfig.saslMechanism {
		case "OAUTHBEARER":
			tp, err := newKgoOauthTokenProvider(ctx, dialConfig)
			if err != nil {
				return nil, err
			}
			sasl = sasloauth.Oauth(tp)
		case "PLAIN", "":
			sasl = saslplain.Plain(func(ctc context.Context) (saslplain.Auth, error) {
				return saslplain.Auth{
					User: dialConfig.saslUser,
					Pass: dialConfig.saslPassword,
				}, nil
			})
		case "SCRAM-SHA-256":
			sasl = saslscram.Sha256(func(ctx context.Context) (saslscram.Auth, error) {
				return saslscram.Auth{
					User: dialConfig.saslUser,
					Pass: dialConfig.saslPassword,
					// IsToken: false,  // ?
				}, nil
			})
		case "SCRAM-SHA-512":
			sasl = saslscram.Sha512(func(ctx context.Context) (saslscram.Auth, error) {
				return saslscram.Auth{
					User: dialConfig.saslUser,
					Pass: dialConfig.saslPassword,
					// IsToken: false,  // ?
				}, nil
			})
		default:
			return nil, errors.Errorf(`unsupported SASL mechanism: %s`, dialConfig.saslMechanism)
		}
		opts = append(opts, kgo.SASL(sasl))
	}

	// Apply some statement level overrides. The flush related ones (Messages, MaxMessages, Bytes) are not applied here, but on the sinkBatchConfig instead.
	sinkCfg, err := getSaramaConfig(jsonStr)
	if err != nil {
		return nil, errors.Wrapf(err,
			"failed to parse sink config; check %s option", changefeedbase.OptKafkaSinkConfig)
	}

	if sinkCfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(sinkCfg.ClientID))
	}

	switch sinkCfg.RequiredAcks {
	case ``, `ONE`:
		// NOTE: idempotency is on by default, but is incompatible with acks<all. TODO: should we have it on by default? i feel like yes
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
	case `ALL`:
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	case `NONE`:
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()), kgo.DisableIdempotentWrite())
	default:
		return nil, errors.Errorf(`unknown required acks value: %s`, sinkCfg.RequiredAcks)
	}

	// TODO: remove this sarama dep
	switch sarama.CompressionCodec(sinkCfg.Compression) {
	case sarama.CompressionNone:
		opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
	case sarama.CompressionGZIP:
		opts = append(opts, kgo.ProducerBatchCompression(kgo.GzipCompression()))
	case sarama.CompressionSnappy:
		opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
	case sarama.CompressionLZ4:
		opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression()))
	case sarama.CompressionZSTD:
		opts = append(opts, kgo.ProducerBatchCompression(kgo.ZstdCompression()))
	default:
		return nil, errors.Errorf(`unknown compression codec: %v`, sinkCfg.Compression)
	}

	if sinkCfg.Version != "" {
		v := kversion.FromString(sinkCfg.Version)
		if v == nil {
			return nil, errors.Errorf(`unknown kafka version: %s`, sinkCfg.Version)
		}
		// TODO: make sure this is right. i think the intention of the setting is just to support really old versions, which is what the Max is for
		opts = append(opts, kgo.MaxVersions(v))
	}

	// TODO: other opts like
	// webhooks', for reference
	// Flush.Messages
	// Flush.Bytes
	// Flush.Frequency
	// Retry.Max
	// Retry.Backoff

	return opts, nil
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

// TODO: this will hit the "used span after finished" thing. probably. consider using a context.Backgound() with stuff on, like in the sarama log adapter
func (k kgoLogAdapter) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	format := `kafka(kgo): %s %s`
	for i := 0; i < len(keyvals); i += 2 {
		format += ` %s=%v`
	}
	log.InfofDepth(k.ctx, 1, format, append([]any{redact.SafeString(level.String()), redact.SafeString(msg)}, keyvals...)...) //nolint:fmtsafe
}

var _ kgo.Logger = kgoLogAdapter{}

// wrappers around the recommended sarama compat approach to pass thru record partitions when key is nil, like current sarama impl
func newKgoChangefeedPartitioner() kgo.Partitioner {
	hasher := fnv.New32a()
	return &kgoChangefeedPartitioner{
		inner: kgo.StickyKeyPartitioner(kgo.SaramaCompatHasher(func(bs []byte) uint32 {
			hasher.Reset()
			_, _ = hasher.Write(bs)
			return hasher.Sum32()
		})),
	}
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
	if r.Key == nil {
		return int(r.Partition)
	}
	return p.inner.Partition(r, n)
}

// OR:
// kgo.BasicConsistentPartitioner(func(topic string) func(r *kgo.Record, n int) int {
// 	// this must (and should) have the same behaviour as our old partitioner which wraps sarama.NewCustomHashPartitioner(fnv.New32a)
// 	hasher := fnv.New32a()
// 	// rand := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
// 	return func(r *kgo.Record, n int) int {
// 		if r.Key == nil {
// 			// TODO: how to handle resolved msgs? the docs suggest that we can't/shouldnt use r.Partition, but
// 			// then there exists the ManualPartitioner which uses it sooooo idk
// 			// return rand.Intn(n)
// 			return int(r.Partition)
// 		}
// 		hasher.Reset()
// 		_, _ = hasher.Write(r.Key)
// 		part := int32(hasher.Sum32()) % int32(n)
// 		if part < 0 {
// 			part = -part
// 		}
// 		return int(part)
// 	}
// })

func newKgoOauthTokenProvider(
	ctx context.Context, dialConfig kafkaDialConfig,
) (func(ctx context.Context) (sasloauth.Auth, error), error) {

	// grant_type is by default going to be set to 'client_credentials' by the
	// clientcredentials library as defined by the spec, however non-compliant
	// auth server implementations may want a custom type
	var endpointParams url.Values
	if dialConfig.saslGrantType != `` {
		endpointParams = url.Values{"grant_type": {dialConfig.saslGrantType}}
	}

	tokenURL, err := url.Parse(dialConfig.saslTokenURL)
	if err != nil {
		return nil, errors.Wrap(err, "malformed token url")
	}

	// the clientcredentials.Config's TokenSource method creates an
	// oauth2.TokenSource implementation which returns tokens for the given
	// endpoint, returning the same cached result until its expiration has been
	// reached, and then once expired re-requesting a new token from the endpoint.
	cfg := clientcredentials.Config{
		ClientID:       dialConfig.saslClientID,
		ClientSecret:   dialConfig.saslClientSecret,
		TokenURL:       tokenURL.String(),
		Scopes:         dialConfig.saslScopes,
		EndpointParams: endpointParams,
	}
	ts := cfg.TokenSource(ctx)

	return func(ctx context.Context) (sasloauth.Auth, error) {
		tok, err := ts.Token()
		if err != nil {
			return sasloauth.Auth{}, err
		}
		return sasloauth.Auth{Token: tok.AccessToken}, nil
	}, nil
}
