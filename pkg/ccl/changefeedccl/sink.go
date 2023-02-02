// Copyright 2018 The Cockroach Authors.
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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
)

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
	return sink, sink.Dial()
}

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
			return makeNullSink(sinkURL{URL: u}, metricsBuilder(nullIsAccounted))
		case u.Scheme == changefeedbase.SinkSchemeKafka:
			return validateOptionsAndMakeSink(changefeedbase.KafkaValidOptions, func() (Sink, error) {
				return makeKafkaSink(ctx, sinkURL{URL: u}, AllTargets(feedCfg), opts.GetKafkaConfigJSON(), serverCfg.Settings, metricsBuilder)
			})
		case isWebhookSink(u):
			webhookOpts, err := opts.GetWebhookSinkOptions()
			if err != nil {
				return nil, err
			}
			return validateOptionsAndMakeSink(changefeedbase.WebhookValidOptions, func() (Sink, error) {
				return makeWebhookSink(ctx, sinkURL{URL: u}, encodingOpts, webhookOpts,
					defaultWorkerCount(), timeutil.DefaultTimeSource{}, metricsBuilder)
			})
		case isPubsubSink(u):
			// TODO: add metrics to pubsubsink
			return MakePubsubSink(ctx, u, encodingOpts, AllTargets(feedCfg))
		case isCloudStorageSink(u):
			return validateOptionsAndMakeSink(changefeedbase.CloudStorageValidOptions, func() (Sink, error) {
				// Placeholder id for canary sink
				var nodeID base.SQLInstanceID = 0
				if serverCfg.NodeID != nil {
					nodeID = serverCfg.NodeID.SQLInstanceID()
				}
				return makeCloudStorageSink(
					ctx, sinkURL{URL: u}, nodeID, serverCfg.Settings, encodingOpts,
					timestampOracle, serverCfg.ExternalStorageFromURI, user, metricsBuilder,
				)
			})
		case u.Scheme == changefeedbase.SinkSchemeExperimentalSQL:
			return validateOptionsAndMakeSink(changefeedbase.SQLValidOptions, func() (Sink, error) {
				return makeSQLSink(sinkURL{URL: u}, sqlSinkTableName, AllTargets(feedCfg), metricsBuilder)
			})
		case u.Scheme == changefeedbase.SinkSchemeExternalConnection:
			return validateOptionsAndMakeSink(changefeedbase.ExternalConnectionValidOptions, func() (Sink, error) {
				return makeExternalConnectionSink(
					ctx, sinkURL{URL: u}, user, serverCfg.DB,
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
		if sinkSpecificOpts != nil {
			if _, ok := sinkSpecificOpts[opt]; ok {
				continue
			}
		}
		return errors.Errorf("this sink is incompatible with option %s", opt)
	}
	return nil
}

// sinkURL is a helper struct which for "consuming" URL query
// parameters from the underlying URL.
type sinkURL struct {
	*url.URL
	q url.Values
}

func (u *sinkURL) consumeParam(p string) string {
	if u.q == nil {
		u.q = u.Query()
	}
	v := u.q.Get(p)
	u.q.Del(p)
	return v
}

func (u *sinkURL) addParam(p string, value string) {
	if u.q == nil {
		u.q = u.Query()
	}
	u.q.Add(p, value)
}

func (u *sinkURL) consumeBool(param string, dest *bool) (wasSet bool, err error) {
	if paramVal := u.consumeParam(param); paramVal != "" {
		wasSet, err := strToBool(paramVal, dest)
		if err != nil {
			return false, errors.Wrapf(err, "param %s must be a bool", param)
		}
		return wasSet, err
	}
	return false, nil
}

func (u *sinkURL) decodeBase64(param string, dest *[]byte) error {
	// TODO(dan): There's a straightforward and unambiguous transformation
	//  between the base 64 encoding defined in RFC 4648 and the URL variant
	//  defined in the same RFC: simply replace all `+` with `-` and `/` with
	//  `_`. Consider always doing this for the user and accepting either
	//  variant.
	val := u.consumeParam(param)
	err := decodeBase64FromString(val, dest)
	if err != nil {
		return errors.Wrapf(err, `param %s must be base 64 encoded`, param)
	}
	return nil
}

func (u *sinkURL) remainingQueryParams() (res []string) {
	for p := range u.q {
		res = append(res, p)
	}
	return
}

func (u *sinkURL) String() string {
	if u.q != nil {
		// If we changed query params, re-encode them.
		u.URL.RawQuery = u.q.Encode()
		u.q = nil
	}
	return u.URL.String()
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
	alloc kvevent.Alloc,
) error {
	if sinkWithEncoder, ok := s.wrapped.(SinkWithEncoder); ok {
		return sinkWithEncoder.EncodeAndEmitRow(ctx, updatedRow, prevRow, topic, updated, mvcc, alloc)
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

func makeNullSink(u sinkURL, m metricsRecorder) (Sink, error) {
	var pacer *time.Ticker
	if delay := u.consumeParam(`delay`); delay != "" {
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
// thread safe. It also has a beforeFlush hook which is called
// at the beginning of safeSink.Flush().
type safeSink struct {
	syncutil.Mutex
	beforeFlush func(ctx context.Context) error
	wrapped     EventSink
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
	if err := s.beforeFlush(ctx); err != nil {
		return err
	}
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
		alloc kvevent.Alloc,
	) error

	Flush(ctx context.Context) error
}
