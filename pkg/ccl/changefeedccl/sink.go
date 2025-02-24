// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"encoding/json"
	"math"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Sink is an abstraction for anything that a changefeed may emit into.
// This union interface is mainly meant for ease of mocking--an individual
// changefeed processor should only need one of these.
type Sink interface {
	EventSink
	ResolvedTimestampSink
}

type sinkType int

const (
	sinkTypeSinklessBuffer sinkType = iota
	sinkTypeNull
	sinkTypeKafka
	sinkTypeWebhook
	sinkTypePubsub
	sinkTypeCloudstorage
	sinkTypeSQL
	sinkTypePulsar
)

func (st sinkType) String() string {
	switch st {
	case sinkTypeSinklessBuffer:
		return `sinkless buffer`
	case sinkTypeNull:
		return `null`
	case sinkTypeKafka:
		return `kafka`
	case sinkTypeWebhook:
		return `webhook`
	case sinkTypePubsub:
		return `pubsub`
	case sinkTypeCloudstorage:
		return `cloudstorage`
	case sinkTypeSQL:
		return `sql`
	case sinkTypePulsar:
		return `pulsar`
	default:
		return `unknown`
	}
}

// externalResource is the interface common to both EventSink and
// ResolvedTimestampSink.
type externalResource interface {
	// Dial establishes a connection to the sink.
	Dial() error

	// Close does not guarantee delivery of outstanding messages.
	// It releases resources and may surface diagnostic information
	// in logs or the returned error.
	Close() error

	// getConcreteType returns the underlying sink type of a sink that may
	// be wrapped by middleware.
	getConcreteType() sinkType
}

// EventSink is the interface used when emitting changefeed events
// and ensuring they were received.
type EventSink interface {
	externalResource

	// EmitRow enqueues a row message for asynchronous delivery on the sink. An
	// error may be returned if a previously enqueued message has failed.
	EmitRow(
		ctx context.Context,
		topic TopicDescriptor,
		key, value []byte,
		updated, mvcc hlc.Timestamp,
		alloc kvevent.Alloc,
	) error

	// Flush blocks until every message enqueued by EmitRow
	// has been acknowledged by the sink. If an error is
	// returned, no guarantees are given about which messages have been
	// delivered or not delivered.
	Flush(ctx context.Context) error
}

// ResolvedTimestampSink is the interface used when emitting resolved
// timestamps.
type ResolvedTimestampSink interface {
	externalResource
	// EmitResolvedTimestamp enqueues a resolved timestamp message for
	// asynchronous delivery on every topic. An error may be returned
	// if a previously enqueued message has failed. Implementations may
	// alternatively emit synchronously.
	EmitResolvedTimestamp(ctx context.Context, encoder Encoder, resolved hlc.Timestamp) error
}

// SinkWithTopics extends the Sink interface to include a method that returns
// the topics that a changefeed will emit to.
type SinkWithTopics interface {
	Topics() []string
}

func getEventSink(
	ctx context.Context,
	serverCfg *execinfra.ServerConfig,
	feedCfg jobspb.ChangefeedDetails,
	timestampOracle timestampLowerBoundOracle,
	user username.SQLUsername,
	jobID jobspb.JobID,
	m metricsRecorder,
) (EventSink, error) {
	return getAndDialSink(ctx, serverCfg, feedCfg, timestampOracle, user, jobID, m)
}

func getResolvedTimestampSink(
	ctx context.Context,
	serverCfg *execinfra.ServerConfig,
	feedCfg jobspb.ChangefeedDetails,
	timestampOracle timestampLowerBoundOracle,
	user username.SQLUsername,
	jobID jobspb.JobID,
	m metricsRecorder,
) (ResolvedTimestampSink, error) {
	return getAndDialSink(ctx, serverCfg, feedCfg, timestampOracle, user, jobID, m)
}

func getAndDialSink(
	ctx context.Context,
	serverCfg *execinfra.ServerConfig,
	feedCfg jobspb.ChangefeedDetails,
	timestampOracle timestampLowerBoundOracle,
	user username.SQLUsername,
	jobID jobspb.JobID,
	m metricsRecorder,
) (Sink, error) {
	sink, err := getSink(ctx, serverCfg, feedCfg, timestampOracle, user, jobID, m)
	if err != nil {
		return nil, err
	}
	if err := sink.Dial(); err != nil {
		if closeErr := sink.Close(); closeErr != nil {
			return nil, errors.CombineErrors(err, errors.Wrap(closeErr, `failed to close sink`))
		}
		return nil, err
	}
	return sink, nil
}

// KafkaV2Enabled determines whether or not the refactored Kafka sink
// or the deprecated sink should be used.
var KafkaV2Enabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"changefeed.new_kafka_sink_enabled",
	"if enabled, this setting enables a new implementation of the kafka sink with improved reliability",
	// TODO(#126991): delete the original kafka sink code
	metamorphic.ConstantWithTestBool("changefeed.new_kafka_sink.enabled", true),
	settings.WithName("changefeed.new_kafka_sink.enabled"),
)

func getSink(
	ctx context.Context,
	serverCfg *execinfra.ServerConfig,
	feedCfg jobspb.ChangefeedDetails,
	timestampOracle timestampLowerBoundOracle,
	user username.SQLUsername,
	jobID jobspb.JobID,
	m metricsRecorder,
) (Sink, error) {
	u, err := url.Parse(feedCfg.SinkURI)
	if err != nil {
		return nil, err
	}
	if scheme, ok := changefeedbase.NoLongerExperimental[u.Scheme]; ok {
		u.Scheme = scheme
	}

	opts := changefeedbase.MakeStatementOptions(feedCfg.Opts)

	// check that options are compatible with the given sink
	validateOptionsAndMakeSink := func(sinkSpecificOpts map[string]struct{}, makeSink func() (Sink, error)) (Sink, error) {
		err := validateSinkOptions(feedCfg.Opts, sinkSpecificOpts)
		if err != nil {
			return nil, err
		}
		return makeSink()
	}

	metricsBuilder := func(recordingRequired bool) metricsRecorder {
		if recordingRequired {
			return maybeWrapMetrics(ctx, m, serverCfg.ExternalIORecorder)
		}
		return m
	}

	newSink := func() (Sink, error) {
		if feedCfg.SinkURI == "" {
			return &bufferSink{metrics: m}, nil
		}

		encodingOpts, err := opts.GetEncodingOptions()
		if err != nil {
			return nil, err
		}

		switch {
		case u.Scheme == changefeedbase.SinkSchemeNull:
			nullIsAccounted := false
			if knobs, ok := serverCfg.TestingKnobs.Changefeed.(*TestingKnobs); ok {
				nullIsAccounted = knobs.NullSinkIsExternalIOAccounted
			}
			return makeNullSink(&changefeedbase.SinkURL{URL: u}, metricsBuilder(nullIsAccounted))
		case isKafkaSink(u):
			return validateOptionsAndMakeSink(changefeedbase.KafkaValidOptions, func() (Sink, error) {
				if KafkaV2Enabled.Get(&serverCfg.Settings.SV) {
					return makeKafkaSinkV2(ctx, &changefeedbase.SinkURL{URL: u}, AllTargets(feedCfg), opts.GetKafkaConfigJSON(),
						numSinkIOWorkers(serverCfg), newCPUPacerFactory(ctx, serverCfg), timeutil.DefaultTimeSource{},
						serverCfg.Settings, metricsBuilder, kafkaSinkV2Knobs{})
				} else {
					return makeKafkaSink(ctx, &changefeedbase.SinkURL{URL: u}, AllTargets(feedCfg), opts.GetKafkaConfigJSON(), serverCfg.Settings, metricsBuilder)
				}
			})
		case isPulsarSink(u):
			var testingKnobs *TestingKnobs
			if knobs, ok := serverCfg.TestingKnobs.Changefeed.(*TestingKnobs); ok {
				testingKnobs = knobs
			}
			return makePulsarSink(ctx, &changefeedbase.SinkURL{URL: u}, encodingOpts, AllTargets(feedCfg), opts.GetKafkaConfigJSON(),
				serverCfg.Settings, metricsBuilder, testingKnobs)
		case isWebhookSink(u):
			webhookOpts, err := opts.GetWebhookSinkOptions()
			if err != nil {
				return nil, err
			}
			return validateOptionsAndMakeSink(changefeedbase.WebhookValidOptions, func() (Sink, error) {
				return makeWebhookSink(ctx, &changefeedbase.SinkURL{URL: u}, encodingOpts, webhookOpts,
					numSinkIOWorkers(serverCfg), newCPUPacerFactory(ctx, serverCfg), timeutil.DefaultTimeSource{},
					metricsBuilder, serverCfg.Settings)
			})
		case isPubsubSink(u):
			var testingKnobs *TestingKnobs
			if knobs, ok := serverCfg.TestingKnobs.Changefeed.(*TestingKnobs); ok {
				testingKnobs = knobs
			}
			return makePubsubSink(ctx, u, encodingOpts, opts.GetPubsubConfigJSON(), AllTargets(feedCfg),
				opts.IsSet(changefeedbase.OptUnordered), numSinkIOWorkers(serverCfg),
				newCPUPacerFactory(ctx, serverCfg), timeutil.DefaultTimeSource{},
				metricsBuilder, serverCfg.Settings, testingKnobs)
		case isCloudStorageSink(u):
			return validateOptionsAndMakeSink(changefeedbase.CloudStorageValidOptions, func() (Sink, error) {
				var testingKnobs *TestingKnobs
				if knobs, ok := serverCfg.TestingKnobs.Changefeed.(*TestingKnobs); ok {
					testingKnobs = knobs
				}

				// Placeholder id for canary sink
				var nodeID base.SQLInstanceID = 0
				if serverCfg.NodeID != nil {
					nodeID = serverCfg.NodeID.SQLInstanceID()
				}
				return makeCloudStorageSink(
					ctx, &changefeedbase.SinkURL{URL: u}, nodeID, serverCfg.Settings, encodingOpts,
					timestampOracle, serverCfg.ExternalStorageFromURI, user, metricsBuilder, testingKnobs,
				)
			})
		case u.Scheme == changefeedbase.SinkSchemeExperimentalSQL:
			return validateOptionsAndMakeSink(changefeedbase.SQLValidOptions, func() (Sink, error) {
				return makeSQLSink(&changefeedbase.SinkURL{URL: u}, sqlSinkTableName, AllTargets(feedCfg), metricsBuilder)
			})
		case u.Scheme == changefeedbase.SinkSchemeExternalConnection:
			return validateOptionsAndMakeSink(changefeedbase.ExternalConnectionValidOptions, func() (Sink, error) {
				return makeExternalConnectionSink(
					ctx, &changefeedbase.SinkURL{URL: u}, user, makeExternalConnectionProvider(ctx, serverCfg.DB),
					serverCfg, feedCfg, timestampOracle, jobID, m,
				)
			})
		case u.Scheme == "":
			return nil, errors.Errorf(`no scheme found for sink URL %q`, feedCfg.SinkURI)
		default:
			return nil, errors.Errorf(`unsupported sink: %s`, u.Scheme)
		}
	}

	sink, err := newSink()
	if err != nil {
		return nil, err
	}

	if knobs, ok := serverCfg.TestingKnobs.Changefeed.(*TestingKnobs); ok && knobs.WrapSink != nil {
		// External connections call getSink recursively and wrap the sink then.
		if u.Scheme != changefeedbase.SinkSchemeExternalConnection {
			sink = knobs.WrapSink(sink, jobID)
		}
	}

	return sink, nil
}

func validateSinkOptions(opts map[string]string, sinkSpecificOpts map[string]struct{}) error {
	for opt := range opts {
		if _, ok := changefeedbase.CommonOptions[opt]; ok {
			continue
		}
		if _, retired := changefeedbase.RetiredOptions[opt]; retired {
			continue
		}
		if sinkSpecificOpts != nil {
			if _, ok := sinkSpecificOpts[opt]; ok {
				continue
			}
		}
		return errors.Errorf("this sink is incompatible with option %s", opt)
	}
	return nil
}

// errorWrapperSink delegates to another sink and marks all returned errors as
// retryable. During changefeed setup, we use the sink once without this to
// verify configuration, but in the steady state, no sink error should be
// terminal.
type errorWrapperSink struct {
	wrapped externalResource
}

func (s *errorWrapperSink) getConcreteType() sinkType {
	return s.wrapped.getConcreteType()
}

// EmitRow implements Sink interface.
func (s errorWrapperSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	if err := s.wrapped.(EventSink).EmitRow(ctx, topic, key, value, updated, mvcc, alloc); err != nil {
		return changefeedbase.MarkRetryableError(err)
	}
	return nil
}

// EmitResolvedTimestamp implements Sink interface.
func (s errorWrapperSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	if err := s.wrapped.(ResolvedTimestampSink).EmitResolvedTimestamp(ctx, encoder, resolved); err != nil {
		return changefeedbase.MarkRetryableError(err)
	}
	return nil
}

// Flush implements Sink interface.
func (s errorWrapperSink) Flush(ctx context.Context) error {
	if err := s.wrapped.(EventSink).Flush(ctx); err != nil {
		return changefeedbase.MarkRetryableError(err)
	}
	return nil
}

// Close implements Sink interface.
func (s errorWrapperSink) Close() error {
	if err := s.wrapped.Close(); err != nil {
		return changefeedbase.MarkRetryableError(err)
	}
	return nil
}

// EncodeAndEmitRow implements SinkWithEncoder interface.
func (s errorWrapperSink) EncodeAndEmitRow(
	ctx context.Context,
	updatedRow cdcevent.Row,
	prevRow cdcevent.Row,
	topic TopicDescriptor,
	updated, mvcc hlc.Timestamp,
	encodingOpts changefeedbase.EncodingOptions,
	alloc kvevent.Alloc,
) error {
	if sinkWithEncoder, ok := s.wrapped.(SinkWithEncoder); ok {
		return sinkWithEncoder.EncodeAndEmitRow(ctx, updatedRow, prevRow, topic, updated, mvcc, encodingOpts, alloc)
	}
	return errors.AssertionFailedf("Expected a sink with encoder for, found %T", s.wrapped)
}

// Dial implements Sink interface.
func (s errorWrapperSink) Dial() error {
	return s.wrapped.Dial()
}

// encDatumRowBuffer is a FIFO of `EncDatumRow`s.
//
// TODO(dan): There's some potential allocation savings here by reusing the same
// backing array.
type encDatumRowBuffer []rowenc.EncDatumRow

func (b *encDatumRowBuffer) IsEmpty() bool {
	return b == nil || len(*b) == 0
}
func (b *encDatumRowBuffer) Push(r rowenc.EncDatumRow) {
	*b = append(*b, r)
}
func (b *encDatumRowBuffer) Pop() rowenc.EncDatumRow {
	ret := (*b)[0]
	*b = (*b)[1:]
	return ret
}

type bufferSink struct {
	buf     encDatumRowBuffer
	alloc   tree.DatumAlloc
	scratch bufalloc.ByteAllocator
	closed  bool
	metrics metricsRecorder
}

func (s *bufferSink) getConcreteType() sinkType {
	return sinkTypeSinklessBuffer
}

// EmitRow implements the Sink interface.
func (s *bufferSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	r kvevent.Alloc,
) error {
	defer r.Release(ctx)
	defer s.metrics.recordOneMessage()(mvcc, len(key)+len(value), sinkDoesNotCompress)

	if s.closed {
		return errors.New(`cannot EmitRow on a closed sink`)
	}

	s.buf.Push(rowenc.EncDatumRow{
		{Datum: tree.DNull}, // resolved span
		{Datum: s.getTopicDatum(topic)},
		{Datum: s.alloc.NewDBytes(tree.DBytes(key))},   // key
		{Datum: s.alloc.NewDBytes(tree.DBytes(value))}, // value
	})
	return nil
}

// EmitResolvedTimestamp implements the Sink interface.
func (s *bufferSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	if s.closed {
		return errors.New(`cannot EmitResolvedTimestamp on a closed sink`)
	}
	defer s.metrics.recordResolvedCallback()()

	var noTopic string
	payload, err := encoder.EncodeResolvedTimestamp(ctx, noTopic, resolved)
	if err != nil {
		return err
	}
	s.scratch, payload = s.scratch.Copy(payload, 0 /* extraCap */)
	s.buf.Push(rowenc.EncDatumRow{
		{Datum: tree.DNull}, // resolved span
		{Datum: tree.DNull}, // topic
		{Datum: tree.DNull}, // key
		{Datum: s.alloc.NewDBytes(tree.DBytes(payload))}, // value
	})
	return nil
}

// Flush implements the Sink interface.
func (s *bufferSink) Flush(_ context.Context) error {
	defer s.metrics.recordFlushRequestCallback()()
	return nil
}

// Close implements the Sink interface.
func (s *bufferSink) Close() error {
	s.closed = true
	return nil
}

// Dial implements the Sink interface.
func (s *bufferSink) Dial() error {
	return nil
}

// TODO (zinger): Make this a tuple or array datum if it can be
// done without breaking backwards compatibility.
func (s *bufferSink) getTopicDatum(t TopicDescriptor) *tree.DString {
	name, components := t.GetNameComponents()
	if len(components) == 0 {
		return s.alloc.NewDString(tree.DString(name))
	}
	strs := append([]string{string(name)}, components...)
	return s.alloc.NewDString(tree.DString(strings.Join(strs, ".")))
}

type nullSink struct {
	ticker  *time.Ticker
	metrics metricsRecorder
}

func (n *nullSink) getConcreteType() sinkType {
	return sinkTypeNull
}

var _ Sink = (*nullSink)(nil)

func makeNullSink(u *changefeedbase.SinkURL, m metricsRecorder) (Sink, error) {
	var pacer *time.Ticker
	if delay := u.ConsumeParam(`delay`); delay != "" {
		pace, err := time.ParseDuration(delay)
		if err != nil {
			return nil, err
		}
		pacer = time.NewTicker(pace)
	}
	return &nullSink{ticker: pacer, metrics: m}, nil
}

func (n *nullSink) pace(ctx context.Context) error {
	if n.ticker != nil {
		select {
		case <-n.ticker.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// EmitRow implements Sink interface.
func (n *nullSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	r kvevent.Alloc,
) error {
	defer r.Release(ctx)
	defer n.metrics.recordOneMessage()(mvcc, len(key)+len(value), sinkDoesNotCompress)
	if err := n.pace(ctx); err != nil {
		return err
	}
	if log.V(2) {
		log.Infof(ctx, "emitting row %s@%s", key, updated.String())
	}
	return nil
}

// EmitResolvedTimestamp implements Sink interface.
func (n *nullSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	defer n.metrics.recordResolvedCallback()()
	if err := n.pace(ctx); err != nil {
		return err
	}
	if log.V(2) {
		log.Infof(ctx, "emitting resolved %s", resolved.String())
	}

	return nil
}

// Flush implements Sink interface.
func (n *nullSink) Flush(ctx context.Context) error {
	defer n.metrics.recordFlushRequestCallback()()
	if log.V(2) {
		log.Info(ctx, "flushing")
	}

	return nil
}

// Close implements Sink interface.
func (n *nullSink) Close() error {
	return nil
}

// Dial implements Sink interface.
func (n *nullSink) Dial() error {
	return nil
}

// safeSink wraps an EventSink in a mutex so it's methods are
// thread safe.
type safeSink struct {
	syncutil.Mutex
	wrapped EventSink
}

var _ EventSink = (*safeSink)(nil)

func (s *safeSink) getConcreteType() sinkType {
	return s.wrapped.getConcreteType()
}

func (s *safeSink) Dial() error {
	s.Lock()
	defer s.Unlock()
	return s.wrapped.Dial()
}

func (s *safeSink) Close() error {
	s.Lock()
	defer s.Unlock()
	return s.wrapped.Close()
}

func (s *safeSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	s.Lock()
	defer s.Unlock()
	return s.wrapped.EmitRow(ctx, topic, key, value, updated, mvcc, alloc)
}

func (s *safeSink) Flush(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()
	return s.wrapped.Flush(ctx)
}

// SinkWithEncoder A sink which both encodes and emits row events. Ideally, this
// should not be embedding the Sink interface because then all the types that
// implement this interface will also have to implement EmitRow (instead, they
// should implement EncodeAndEmitRow), which for a sink with encoder, does not
// make sense. But since we pass around a type of sink everywhere, we need to
// embed this for now.
type SinkWithEncoder interface {
	Sink

	EncodeAndEmitRow(
		ctx context.Context,
		updatedRow cdcevent.Row,
		prevRow cdcevent.Row,
		topic TopicDescriptor,
		updated, mvcc hlc.Timestamp,
		encodingOpts changefeedbase.EncodingOptions,
		alloc kvevent.Alloc,
	) error

	Flush(ctx context.Context) error
}

// proper JSON schema for sink config:
//
//	{
//	  "Flush": {
//		   "Messages":  ...,
//		   "Bytes":     ...,
//		   "Frequency": ...,
//	  },
//		 "Retry": {
//		   "Max":     ...,
//		   "Backoff": ...,
//	  }
//	}
type sinkJSONConfig struct {
	Flush sinkBatchConfig `json:",omitempty"`
	Retry sinkRetryConfig `json:",omitempty"`
}

type sinkBatchConfig struct {
	Bytes, Messages int          `json:",omitempty"`
	Frequency       jsonDuration `json:",omitempty"`
}

// wrapper structs to unmarshal json, retry.Options will be the actual config
type sinkRetryConfig struct {
	Max     jsonMaxRetries `json:",omitempty"`
	Backoff jsonDuration   `json:",omitempty"`
}

func defaultRetryConfig() retry.Options {
	opts := retry.Options{
		InitialBackoff: 500 * time.Millisecond,
		MaxRetries:     3,
		Multiplier:     2,
	}
	// max backoff should be initial * 2 ^ maxRetries
	opts.MaxBackoff = opts.InitialBackoff * time.Duration(int(math.Pow(2.0, float64(opts.MaxRetries))))
	return opts
}

func getSinkConfigFromJson(
	jsonStr changefeedbase.SinkSpecificJSONConfig, baseConfig sinkJSONConfig,
) (batchCfg sinkBatchConfig, retryCfg retry.Options, err error) {
	retryCfg = defaultRetryConfig()

	var cfg = baseConfig
	cfg.Retry.Max = jsonMaxRetries(retryCfg.MaxRetries)
	cfg.Retry.Backoff = jsonDuration(retryCfg.InitialBackoff)
	if jsonStr != `` {
		// set retry defaults to be overridden if included in JSON
		if err = json.Unmarshal([]byte(jsonStr), &cfg); err != nil {
			return batchCfg, retryCfg, errors.Wrapf(err, "error unmarshalling json")
		}
	}

	// don't support negative values
	if cfg.Flush.Messages < 0 || cfg.Flush.Bytes < 0 || cfg.Flush.Frequency < 0 ||
		cfg.Retry.Max < 0 || cfg.Retry.Backoff < 0 {
		return batchCfg, retryCfg, errors.Errorf("invalid sink config, all values must be non-negative")
	}

	// errors if other batch values are set, but frequency is not
	if (cfg.Flush.Messages > 0 || cfg.Flush.Bytes > 0) && cfg.Flush.Frequency == 0 {
		return batchCfg, retryCfg, errors.Errorf("invalid sink config, Flush.Frequency is not set, messages may never be sent")
	}

	retryCfg.MaxRetries = int(cfg.Retry.Max)
	retryCfg.InitialBackoff = time.Duration(cfg.Retry.Backoff)
	retryCfg.MaxBackoff = 30 * time.Second
	return cfg.Flush, retryCfg, nil
}

type jsonMaxRetries int

func (j *jsonMaxRetries) UnmarshalJSON(b []byte) error {
	var i int64
	// try to parse as int
	i, err := strconv.ParseInt(string(b), 10, 64)
	if err == nil {
		if i <= 0 {
			return errors.Errorf("Retry.Max must be a positive integer. use 'inf' for infinite retries.")
		}
		*j = jsonMaxRetries(i)
	} else {
		// if that fails, try to parse as string (only accept 'inf')
		var s string
		// using unmarshal here to remove quotes around the string
		if err := json.Unmarshal(b, &s); err != nil {
			return errors.Errorf("Retry.Max must be either a positive int or 'inf' for infinite retries.")
		}
		if strings.ToLower(s) == "inf" {
			// if used wants infinite retries, set to zero as retry.Options interprets this as infinity
			*j = 0
		} else if n, err := strconv.Atoi(s); err == nil { // also accept ints as strings
			*j = jsonMaxRetries(n)
		} else {
			return errors.Errorf("Retry.Max must be either a positive int or 'inf' for infinite retries.")
		}
	}
	return nil
}

func numSinkIOWorkers(cfg *execinfra.ServerConfig) int {
	numWorkers := changefeedbase.SinkIOWorkers.Get(&cfg.Settings.SV)
	if numWorkers > 0 {
		return int(numWorkers)
	}

	idealNumber := runtime.GOMAXPROCS(0)
	if idealNumber < 1 {
		return 1
	}
	if idealNumber > 32 {
		return 32
	}
	return idealNumber
}

func newCPUPacerFactory(ctx context.Context, cfg *execinfra.ServerConfig) func() *admission.Pacer {
	var prevPacer *admission.Pacer
	var prevRequestUnit time.Duration

	return func() *admission.Pacer {
		pacerRequestUnit := changefeedbase.SinkPacerRequestSize.Get(&cfg.Settings.SV)
		enablePacer := changefeedbase.PerEventElasticCPUControlEnabled.Get(&cfg.Settings.SV)

		if enablePacer && prevPacer != nil && prevRequestUnit == pacerRequestUnit {
			return prevPacer
		}

		var pacer *admission.Pacer = nil
		if enablePacer {
			tenantID, ok := roachpb.ClientTenantFromContext(ctx)
			if !ok {
				tenantID = roachpb.SystemTenantID
			}

			pacer = cfg.AdmissionPacerFactory.NewPacer(
				pacerRequestUnit,
				admission.WorkInfo{
					TenantID:        tenantID,
					Priority:        admissionpb.BulkNormalPri,
					CreateTime:      timeutil.Now().UnixNano(),
					BypassAdmission: false,
				},
			)
		}

		prevPacer = pacer
		prevRequestUnit = pacerRequestUnit

		return pacer
	}
}

func nilPacerFactory() *admission.Pacer {
	return nil
}

func shouldFlushBatch(bytes int, messages int, config sinkBatchConfig) bool {
	switch {
	// all zero values is interpreted as flush every time
	case config.Messages == 0 && config.Bytes == 0 && config.Frequency == 0:
		return true
	// messages threshold has been reached
	case config.Messages > 0 && messages >= config.Messages:
		return true
	// bytes threshold has been reached
	case config.Bytes > 0 && bytes >= config.Bytes:
		return true
	default:
		return false
	}
}

func sinkSupportsConcurrentEmits(sink EventSink) bool {
	_, ok := sink.(*batchingSink)
	return ok
}
