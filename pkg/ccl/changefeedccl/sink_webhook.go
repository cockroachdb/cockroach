// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/system"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	applicationTypeJSON = `application/json`
	authorizationHeader = `Authorization`
	defaultConnTimeout  = 3 * time.Second
)

func isWebhookSink(u *url.URL) bool {
	switch u.Scheme {
	// allow HTTP here but throw an error later to make it clear HTTPS is required
	case changefeedbase.SinkSchemeWebhookHTTP, changefeedbase.SinkSchemeWebhookHTTPS:
		return true
	default:
		return false
	}
}

type webhookSinkPayload struct {
	Payload []json.RawMessage `json:"payload"`
	Length  int               `json:"length"`
}

func encodePayloadWebhook(values []webhookMessage) ([]byte, error) {
	payload := make([]json.RawMessage, len(values))
	for i, value := range values {
		payload[i] = value.m
	}
	body := &webhookSinkPayload{
		Payload: payload,
		Length:  len(payload),
	}
	j, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return j, err
}

type webhookMessage struct {
	// include key to send to worker
	k []byte
	m []byte
	a kvevent.Alloc
}

type batch struct {
	buffer                      []webhookMessage
	bufferBytes, bufferMsgCount int
}

func (b *batch) addToBuffer(payload webhookMessage) {
	b.bufferMsgCount++
	b.bufferBytes += len(payload.m)
	b.buffer = append(b.buffer, payload)
}

func (b *batch) reset() {
	b.buffer = b.buffer[:0]
	b.bufferMsgCount = 0
	b.bufferBytes = 0
}

type batchConfig struct {
	Bytes, Messages int          `json:",omitempty"`
	Frequency       jsonDuration `json:",omitempty"`
}

type webhookSink struct {
	workerCtx   context.Context
	url         sinkURL
	authHeader  string
	parallelism int
	client      *httputil.Client
	workerGroup ctxgroup.Group
	exitWorkers func()
	eventsChans []chan []webhookMessage
	inflight    *inflightTracker
	retryCfg    retry.Options
	batchCfg    batchConfig
	ts          timeutil.TimeSource
	batchChan   chan webhookMessage
}

// wrapper structs to unmarshal json, retry.Options will be the actual config
type retryConfig struct {
	Max     int          `json:",omitempty"`
	Backoff jsonDuration `json:",omitempty"`
}

// proper JSON schema for webhook sink config:
// {
//   "Flush": {
//	   "Messages":  ...,
//	   "Bytes":     ...,
//	   "Frequency": ...,
//   },
//	 "Retry": {
//	   "Max":     ...,
//	   "Backoff": ...,
//   }
// }
type webhookSinkConfig struct {
	Flush batchConfig `json:",omitempty"`
	Retry retryConfig `json:",omitempty"`
}

func (s *webhookSink) getWebhookSinkConfig(
	opts map[string]string,
) (batchCfg batchConfig, retryCfg retry.Options, err error) {
	retryCfg = defaultRetryConfig()

	var cfg webhookSinkConfig
	if configStr, ok := opts[changefeedbase.OptWebhookSinkConfig]; ok {
		// set retry defaults to be overridden if included in JSON
		cfg.Retry.Max = retryCfg.MaxRetries
		cfg.Retry.Backoff = jsonDuration(retryCfg.InitialBackoff)
		if err = json.Unmarshal([]byte(configStr), &cfg); err != nil {
			return batchCfg, retryCfg, errors.Wrapf(err, "error unmarshalling json")
		}
	}

	// don't support negative values
	if cfg.Flush.Messages < 0 || cfg.Flush.Bytes < 0 || cfg.Flush.Frequency < 0 ||
		cfg.Retry.Max < 0 || cfg.Retry.Backoff < 0 {
		return batchCfg, retryCfg, errors.Errorf("invalid option value %s, all config values must be non-negative", changefeedbase.OptWebhookSinkConfig)
	}

	// errors if other batch values are set, but frequency is not
	if (cfg.Flush.Messages > 0 || cfg.Flush.Bytes > 0) && cfg.Flush.Frequency == 0 {
		return batchCfg, retryCfg, errors.Errorf("invalid option value %s, flush frequency is not set, messages may never be sent", changefeedbase.OptWebhookSinkConfig)
	}

	retryCfg.MaxRetries = cfg.Retry.Max
	retryCfg.InitialBackoff = time.Duration(cfg.Retry.Backoff)
	return cfg.Flush, retryCfg, nil
}

func makeWebhookSink(
	ctx context.Context,
	u sinkURL,
	opts map[string]string,
	parallelism int,
	source timeutil.TimeSource,
) (Sink, error) {
	if u.Scheme != changefeedbase.SinkSchemeWebhookHTTPS {
		return nil, errors.Errorf(`this sink requires %s`, changefeedbase.SinkSchemeHTTPS)
	}
	u.Scheme = strings.TrimPrefix(u.Scheme, `webhook-`)

	switch changefeedbase.FormatType(opts[changefeedbase.OptFormat]) {
	case changefeedbase.OptFormatJSON:
	// only JSON supported at this time for webhook sink
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptFormat, opts[changefeedbase.OptFormat])
	}

	switch changefeedbase.EnvelopeType(opts[changefeedbase.OptEnvelope]) {
	case changefeedbase.OptEnvelopeWrapped:
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptEnvelope, opts[changefeedbase.OptEnvelope])
	}

	if _, ok := opts[changefeedbase.OptKeyInValue]; !ok {
		return nil, errors.Errorf(`this sink requires the WITH %s option`, changefeedbase.OptKeyInValue)
	}

	if _, ok := opts[changefeedbase.OptTopicInValue]; !ok {
		return nil, errors.Errorf(`this sink requires the WITH %s option`, changefeedbase.OptTopicInValue)
	}

	var connTimeout time.Duration
	if timeout, ok := opts[changefeedbase.OptWebhookClientTimeout]; ok {
		var err error
		connTimeout, err = time.ParseDuration(timeout)
		if err != nil {
			return nil, errors.Wrapf(err, "problem parsing option %s", changefeedbase.OptWebhookClientTimeout)
		} else if connTimeout <= time.Duration(0) {
			return nil, fmt.Errorf("option %s must be a positive duration", changefeedbase.OptWebhookClientTimeout)
		}
	} else {
		connTimeout = defaultConnTimeout
	}

	ctx, cancel := context.WithCancel(ctx)

	sink := &webhookSink{
		workerCtx:   ctx,
		authHeader:  opts[changefeedbase.OptWebhookAuthHeader],
		exitWorkers: cancel,
		parallelism: parallelism,
		ts:          source,
	}

	var err error
	sink.batchCfg, sink.retryCfg, err = sink.getWebhookSinkConfig(opts)
	if err != nil {
		return nil, errors.Wrapf(err, "error processing option %s", changefeedbase.OptWebhookSinkConfig)
	}

	sink.client, err = makeWebhookClient(u, connTimeout)
	if err != nil {
		return nil, err
	}

	// remove known query params from sink URL before setting in sink config
	sinkURLParsed, err := url.Parse(u.String())
	if err != nil {
		return nil, err
	}
	params := sinkURLParsed.Query()
	params.Del(changefeedbase.SinkParamSkipTLSVerify)
	params.Del(changefeedbase.SinkParamCACert)
	sinkURLParsed.RawQuery = params.Encode()
	sink.url = sinkURL{URL: sinkURLParsed}

	sink.inflight = makeInflightTracker()
	return sink, nil
}

func makeWebhookClient(u sinkURL, timeout time.Duration) (*httputil.Client, error) {
	client := &httputil.Client{
		Client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{Timeout: timeout}).DialContext,
			},
		},
	}

	dialConfig := struct {
		tlsSkipVerify bool
		caCert        []byte
		clientCert    []byte
		clientKey     []byte
	}{}

	transport := client.Transport.(*http.Transport)

	if _, err := u.consumeBool(changefeedbase.SinkParamSkipTLSVerify, &dialConfig.tlsSkipVerify); err != nil {
		return nil, err
	}
	if err := u.decodeBase64(changefeedbase.SinkParamCACert, &dialConfig.caCert); err != nil {
		return nil, err
	}

	transport.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: dialConfig.tlsSkipVerify,
	}

	if dialConfig.caCert != nil {
		caCertPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, errors.Wrap(err, "could not load system root CA pool")
		}
		if caCertPool == nil {
			caCertPool = x509.NewCertPool()
		}
		if !caCertPool.AppendCertsFromPEM(dialConfig.caCert) {
			return nil, errors.Errorf("failed to parse certificate data:%s", string(dialConfig.caCert))
		}
		transport.TLSClientConfig.RootCAs = caCertPool
	}

	return client, nil
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

// defaultWorkerCount() is the number of CPU's on the machine
func defaultWorkerCount() int {
	return system.NumCPU()
}

func (s *webhookSink) setupWorkers() {
	// setup events channels to send to workers and the worker group
	s.eventsChans = make([]chan []webhookMessage, s.parallelism)
	s.workerGroup = ctxgroup.WithContext(s.workerCtx)
	s.batchChan = make(chan webhookMessage)
	s.workerGroup.GoCtx(func(ctx context.Context) error {
		s.batchWorker()
		return nil
	})
	for i := 0; i < s.parallelism; i++ {
		s.eventsChans[i] = make(chan []webhookMessage)
		j := i
		s.workerGroup.GoCtx(func(ctx context.Context) error {
			s.workerLoop(j)
			return nil
		})
	}
}

func (s *webhookSink) shouldSendBatch(b batch) bool {
	// similar to sarama, send batch if:
	// everything is zero (default)
	// any one of the conditions are met UNLESS the condition is zero which means never batch
	switch {
	// all zero values should batch every time, otherwise batch will wait forever
	case s.batchCfg.Messages == 0 && s.batchCfg.Bytes == 0 && s.batchCfg.Frequency == 0:
		return true
	// messages threshold has been reached
	case s.batchCfg.Messages > 0 && b.bufferMsgCount >= s.batchCfg.Messages:
		return true
	// bytes threshold has been reached
	case s.batchCfg.Bytes > 0 && b.bufferBytes >= s.batchCfg.Bytes:
		return true
	default:
		return false
	}
}

func (s *webhookSink) splitAndSendBatch(batch []webhookMessage) error {
	workerBatches := make([][]webhookMessage, s.parallelism)
	for _, msg := range batch {
		// split batch into per-worker batches
		i := s.workerIndex(msg.k)
		workerBatches[i] = append(workerBatches[i], msg)
	}
	for i, workerBatch := range workerBatches {
		// don't send empty batches
		if len(workerBatch) > 0 {
			select {
			case <-s.workerCtx.Done():
				return s.workerCtx.Err()
			case s.eventsChans[i] <- workerBatch:
			}
		}
	}
	return nil
}

// batchWorker ingests messages from EmitRow into a batch and splits them into
// per-worker batches to be sent separately
func (s *webhookSink) batchWorker() {
	var batchTracker batch
	batchTimer := s.ts.NewTimer()
	defer batchTimer.Stop()
	for {
		select {
		case <-s.workerCtx.Done():
			return
		case msg := <-s.batchChan:
			batchTracker.addToBuffer(msg)
			if s.shouldSendBatch(batchTracker) {
				err := s.splitAndSendBatch(batchTracker.buffer)
				s.inflight.maybeSetError(err)
				batchTracker.reset()
			} else {
				if len(batchTracker.buffer) == 1 && time.Duration(s.batchCfg.Frequency) > 0 {
					// only start timer when first message appears
					batchTimer.Reset(time.Duration(s.batchCfg.Frequency))
				}
			}
		// check the channel for time expiry. the batch should have at least one
		// message in it. If it doesn't, the timer has been carried over from a
		// previous batch, and the new batch will be empty so it won't send. If
		// the new batch has at least one element, the timer will be reset so it'll
		// be updated.
		case <-batchTimer.Ch():
			batchTimer.MarkRead()
			if len(batchTracker.buffer) > 0 {
				err := s.splitAndSendBatch(batchTracker.buffer)
				s.inflight.maybeSetError(err)
				batchTracker.reset()
			}
		}
	}
}

func (s *webhookSink) workerLoop(workerIndex int) {
	for {
		select {
		case <-s.workerCtx.Done():
			return
		case msgs := <-s.eventsChans[workerIndex]:
			encodedMsgs, err := encodePayloadWebhook(msgs)
			if err == nil {
				err = s.sendMessageWithRetries(s.workerCtx, encodedMsgs)
			}
			s.inflight.maybeSetError(err)
			for _, m := range msgs {
				// reduce inflight count by one and reduce memory counter
				s.inflight.FinishRequest(s.workerCtx, m.a)
			}
			// shut down all other workers immediately if error encountered
			if err != nil {
				s.exitWorkers()
				return
			}
		}
	}
}

func (s *webhookSink) Dial() error {
	s.setupWorkers()
	return nil
}

func (s *webhookSink) sendMessageWithRetries(ctx context.Context, reqBody []byte) error {
	requestFunc := func() error {
		return s.sendMessage(ctx, reqBody)
	}

	err := retry.WithMaxAttempts(ctx, s.retryCfg, s.retryCfg.MaxRetries+1, requestFunc)
	return err
}

func (s *webhookSink) sendMessage(ctx context.Context, reqBody []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url.String(), bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", applicationTypeJSON)
	if s.authHeader != "" {
		req.Header.Set(authorizationHeader, s.authHeader)
	}

	var res *http.Response
	res, err = s.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if !(res.StatusCode >= http.StatusOK && res.StatusCode < http.StatusMultipleChoices) {
		resBody, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return errors.Wrapf(err, "failed to read body for HTTP response with status: %d", res.StatusCode)
		}
		return fmt.Errorf("%s: %s", res.Status, string(resBody))
	}
	return nil
}

// workerIndex assigns rows each to a worker goroutine based on the hash of its
// primary key. This is to ensure that each message with the same key gets
// deterministically assigned to the same worker. Since we have a channel per
// worker, we can ensure per-worker ordering and therefore guarantee per-key
// ordering.
func (s *webhookSink) workerIndex(key []byte) uint32 {
	return crc32.ChecksumIEEE(key) % uint32(s.parallelism)
}

// inflightTracker wraps logic for counting number of inflight messages to
// track when flushing sink, with error handling and memory monitoring
// functionality. Implemented as a wrapper for WaitGroup and lock to block
// additional messages while flushing.
type inflightTracker struct {
	inflightGroup sync.WaitGroup
	errChan       chan error
	// lock used here to allow flush to block any new messages being enqueued
	// from EmitRow or EmitResolvedTimestamp
	flushMu syncutil.Mutex
}

func makeInflightTracker() *inflightTracker {
	inflight := &inflightTracker{
		inflightGroup: sync.WaitGroup{},
		errChan:       make(chan error, 1),
	}
	return inflight
}

// maybeSetError sets flushErr to be err if it has not already been set.
func (i *inflightTracker) maybeSetError(err error) {
	if err == nil {
		return
	}
	// errChan has buffer size 1, first error will be saved to the buffer and
	// subsequent errors will be ignored
	select {
	case i.errChan <- err:
	default:
	}
}

// hasError checks if inflightTracker has an error on the buffer and returns
// error if exists.
func (i *inflightTracker) hasError() error {
	var err error
	select {
	case err = <-i.errChan:
	default:
	}
	return err
}

// StartRequest enqueues one inflight message to be flushed.
func (i *inflightTracker) StartRequest() {
	i.flushMu.Lock()
	defer i.flushMu.Unlock()
	i.inflightGroup.Add(1)
}

// FinishRequest tells the inflight tracker one message has been delivered.
func (i *inflightTracker) FinishRequest(ctx context.Context, alloc kvevent.Alloc) {
	alloc.Release(ctx)
	i.inflightGroup.Done()
}

// Wait waits for all inflight messages to be delivered (inflight = 0) and
// returns a possible error. New messages delivered via EmitRow and
// EmitResolvedTimestamp will be blocked until this returns.
func (i *inflightTracker) Wait(ctx context.Context) error {
	i.flushMu.Lock()
	defer i.flushMu.Unlock()
	i.inflightGroup.Wait()
	var err error
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-i.errChan:
	default:
	}
	return err
}

func (i *inflightTracker) Close(ctx context.Context) {
	close(i.errChan)
}

func (s *webhookSink) EmitRow(
	ctx context.Context, _ TopicDescriptor, key, value []byte, _ hlc.Timestamp, alloc kvevent.Alloc,
) error {

	// check if error has been encountered and exit if needed
	err := s.inflight.hasError()
	if err != nil {
		return err
	}

	s.inflight.StartRequest()

	select {
	// check the webhook sink context in case workers have been terminated
	case <-s.workerCtx.Done():
		return s.workerCtx.Err()
	case <-ctx.Done():
		return ctx.Err()
	case s.batchChan <- webhookMessage{k: key, m: value, a: alloc}:
	}
	return nil
}

func (s *webhookSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	payload, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
	if err != nil {
		return err
	}

	select {
	// check the webhook sink context in case workers have been terminated
	case <-s.workerCtx.Done():
		return s.workerCtx.Err()
	// non-blocking check for error, restart changefeed if encountered
	case <-s.inflight.errChan:
		return err
	default:
	}

	s.inflight.StartRequest()

	// do worker logic directly here instead (there's no point using workers for
	// resolved timestamps since there are no keys and everything must be
	// in order)
	err = s.sendMessageWithRetries(ctx, payload)
	s.inflight.maybeSetError(err)
	s.inflight.FinishRequest(ctx, kvevent.Alloc{})
	return err
}

func (s *webhookSink) Flush(ctx context.Context) error {
	return s.inflight.Wait(ctx)
}

func (s *webhookSink) Close() error {
	s.exitWorkers()
	// ignore errors here since we're closing the sink anyway
	_ = s.workerGroup.Wait()
	close(s.batchChan)
	s.inflight.Close(s.workerCtx)
	for _, eventsChan := range s.eventsChans {
		close(eventsChan)
	}
	s.client.CloseIdleConnections()
	return nil
}

// redactWebhookAuthHeader redacts sensitive information from `auth`, which
// should be the value of the HTTP header `Authorization:`. The entire header
// should be redacted here. Wrapped in a function so we can change the
// redaction strategy if needed.
func redactWebhookAuthHeader(_ string) string {
	return "redacted"
}
