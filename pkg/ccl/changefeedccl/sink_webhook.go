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
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/system"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	applicationTypeJSON = `application/json`
	applicationTypeCSV  = `text/csv`
	authorizationHeader = `Authorization`
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

type webhookSink struct {
	// Webhook configuration.
	parallelism int
	retryCfg    retry.Options
	batchCfg    batchConfig
	ts          timeutil.TimeSource
	format      changefeedbase.FormatType

	// Webhook destination.
	url        sinkURL
	authHeader string
	client     *httputil.Client

	// messages are written onto batch channel
	// which batches matches based on batching configuration.
	batchChan chan webhookMessage

	// flushDone channel signaled when flushing completes.
	flushDone chan struct{}

	// errChan is written to indicate an error while sending message.
	errChan chan error

	// parallelism workers are created and controlled by the workerGroup, running with workerCtx.
	// each worker gets its own events channel.
	workerCtx   context.Context
	workerGroup ctxgroup.Group
	exitWorkers func() // Signaled to shut down all workers.
	eventsChans []chan []messagePayload
	metrics     metricsRecorder
}

func (s *webhookSink) getConcreteType() sinkType {
	return sinkTypeWebhook
}

type webhookSinkPayload struct {
	Payload []json.RawMessage `json:"payload"`
	Length  int               `json:"length"`
}

type encodedPayload struct {
	data     []byte
	alloc    kvevent.Alloc
	emitTime time.Time
	mvcc     hlc.Timestamp
}

func encodePayloadJSONWebhook(messages []messagePayload) (encodedPayload, error) {
	result := encodedPayload{
		emitTime: timeutil.Now(),
	}

	payload := make([]json.RawMessage, len(messages))
	for i, m := range messages {
		result.alloc.Merge(&m.alloc)
		payload[i] = m.val
		if m.emitTime.Before(result.emitTime) {
			result.emitTime = m.emitTime
		}
		if result.mvcc.IsEmpty() || m.mvcc.Less(result.mvcc) {
			result.mvcc = m.mvcc
		}
	}

	body := &webhookSinkPayload{
		Payload: payload,
		Length:  len(payload),
	}
	j, err := json.Marshal(body)
	if err != nil {
		return encodedPayload{}, err
	}
	result.data = j
	return result, err
}

func encodePayloadCSVWebhook(messages []messagePayload) (encodedPayload, error) {
	result := encodedPayload{
		emitTime: timeutil.Now(),
	}

	var mergedMsgs []byte
	for _, m := range messages {
		result.alloc.Merge(&m.alloc)
		mergedMsgs = append(mergedMsgs, m.val...)
		if m.emitTime.Before(result.emitTime) {
			result.emitTime = m.emitTime
		}
		if result.mvcc.IsEmpty() || m.mvcc.Less(result.mvcc) {
			result.mvcc = m.mvcc
		}
	}

	result.data = mergedMsgs
	return result, nil
}

type messagePayload struct {
	// Payload message fields.
	key      []byte
	val      []byte
	alloc    kvevent.Alloc
	emitTime time.Time
	mvcc     hlc.Timestamp
}

// webhookMessage contains either messagePayload or a flush request.
type webhookMessage struct {
	flushDone *chan struct{}
	payload   messagePayload
}

type batch struct {
	buffer      []messagePayload
	bufferBytes int
}

func (b *batch) addToBuffer(m messagePayload) {
	b.bufferBytes += len(m.val)
	b.buffer = append(b.buffer, m)
}

func (b *batch) reset() {
	b.buffer = b.buffer[:0]
	b.bufferBytes = 0
}

type batchConfig struct {
	Bytes, Messages int          `json:",omitempty"`
	Frequency       jsonDuration `json:",omitempty"`
}

type jsonMaxRetries int

func (j *jsonMaxRetries) UnmarshalJSON(b []byte) error {
	var i int64
	// try to parse as int
	i, err := strconv.ParseInt(string(b), 10, 64)
	if err == nil {
		if i <= 0 {
			return errors.Errorf("max retry count must be a positive integer. use 'inf' for infinite retries.")
		}
		*j = jsonMaxRetries(i)
	} else {
		// if that fails, try to parse as string (only accept 'inf')
		var s string
		// using unmarshal here to remove quotes around the string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		if strings.ToLower(s) == "inf" {
			// if used wants infinite retries, set to zero as retry.Options interprets this as infinity
			*j = 0
		} else if n, err := strconv.Atoi(s); err == nil { // also accept ints as strings
			*j = jsonMaxRetries(n)
		} else {
			return errors.Errorf("max retries must be either a positive int or 'inf' for infinite retries.")
		}
	}
	return nil
}

// wrapper structs to unmarshal json, retry.Options will be the actual config
type retryConfig struct {
	Max     jsonMaxRetries `json:",omitempty"`
	Backoff jsonDuration   `json:",omitempty"`
}

// proper JSON schema for webhook sink config:
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
type webhookSinkConfig struct {
	Flush batchConfig `json:",omitempty"`
	Retry retryConfig `json:",omitempty"`
}

func (s *webhookSink) getWebhookSinkConfig(
	jsonStr changefeedbase.SinkSpecificJSONConfig,
) (batchCfg batchConfig, retryCfg retry.Options, err error) {
	retryCfg = defaultRetryConfig()

	var cfg webhookSinkConfig
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
		return batchCfg, retryCfg, errors.Errorf("invalid option value %s, all config values must be non-negative", changefeedbase.OptWebhookSinkConfig)
	}

	// errors if other batch values are set, but frequency is not
	if (cfg.Flush.Messages > 0 || cfg.Flush.Bytes > 0) && cfg.Flush.Frequency == 0 {
		return batchCfg, retryCfg, errors.Errorf("invalid option value %s, flush frequency is not set, messages may never be sent", changefeedbase.OptWebhookSinkConfig)
	}

	retryCfg.MaxRetries = int(cfg.Retry.Max)
	retryCfg.InitialBackoff = time.Duration(cfg.Retry.Backoff)
	return cfg.Flush, retryCfg, nil
}

func makeWebhookSink(
	ctx context.Context,
	u sinkURL,
	encodingOpts changefeedbase.EncodingOptions,
	opts changefeedbase.WebhookSinkOptions,
	parallelism int,
	source timeutil.TimeSource,
	mb metricsRecorderBuilder,
) (Sink, error) {
	if u.Scheme != changefeedbase.SinkSchemeWebhookHTTPS {
		return nil, errors.Errorf(`this sink requires %s`, changefeedbase.SinkSchemeHTTPS)
	}
	u.Scheme = strings.TrimPrefix(u.Scheme, `webhook-`)

	switch encodingOpts.Format {
	case changefeedbase.OptFormatJSON:
	case changefeedbase.OptFormatCSV:
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptFormat, encodingOpts.Format)
	}

	switch encodingOpts.Envelope {
	case changefeedbase.OptEnvelopeWrapped, changefeedbase.OptEnvelopeBare:
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptEnvelope, encodingOpts.Envelope)
	}

	if encodingOpts.Envelope != changefeedbase.OptEnvelopeBare {
		encodingOpts.KeyInValue = true
		encodingOpts.TopicInValue = true
	}

	var connTimeout time.Duration
	if opts.ClientTimeout != nil {
		connTimeout = *opts.ClientTimeout
	}

	ctx, cancel := context.WithCancel(ctx)

	sink := &webhookSink{
		workerCtx:   ctx,
		authHeader:  opts.AuthHeader,
		exitWorkers: cancel,
		parallelism: parallelism,
		ts:          source,
		metrics:     mb(requiresResourceAccounting),
		format:      encodingOpts.Format,
	}

	var err error
	sink.batchCfg, sink.retryCfg, err = sink.getWebhookSinkConfig(opts.JSONConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "error processing option %s", changefeedbase.OptWebhookSinkConfig)
	}

	// TODO(yevgeniy): Establish HTTP connection in Dial().
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
	params.Del(changefeedbase.SinkParamClientCert)
	params.Del(changefeedbase.SinkParamClientKey)
	sinkURLParsed.RawQuery = params.Encode()
	sink.url = sinkURL{URL: sinkURLParsed}

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
	if err := u.decodeBase64(changefeedbase.SinkParamClientCert, &dialConfig.clientCert); err != nil {
		return nil, err
	}
	if err := u.decodeBase64(changefeedbase.SinkParamClientKey, &dialConfig.clientKey); err != nil {
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
		transport.TLSClientConfig.Certificates = []tls.Certificate{cert}
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

func (s *webhookSink) Dial() error {
	s.setupWorkers()
	return nil
}

func (s *webhookSink) setupWorkers() {
	// setup events channels to send to workers and the worker group
	s.eventsChans = make([]chan []messagePayload, s.parallelism)
	s.workerGroup = ctxgroup.WithContext(s.workerCtx)
	s.batchChan = make(chan webhookMessage)

	// an error channel with buffer for the first error.
	s.errChan = make(chan error, 1)

	// flushDone notified when flush completes.
	s.flushDone = make(chan struct{})

	s.workerGroup.GoCtx(func(ctx context.Context) error {
		s.batchWorker()
		return nil
	})
	for i := 0; i < s.parallelism; i++ {
		s.eventsChans[i] = make(chan []messagePayload)
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
	case s.batchCfg.Messages > 0 && len(b.buffer) >= s.batchCfg.Messages:
		return true
	// bytes threshold has been reached
	case s.batchCfg.Bytes > 0 && b.bufferBytes >= s.batchCfg.Bytes:
		return true
	default:
		return false
	}
}

func (s *webhookSink) splitAndSendBatch(batch []messagePayload) error {
	workerBatches := make([][]messagePayload, s.parallelism)
	for _, msg := range batch {
		// split batch into per-worker batches
		i := s.workerIndex(msg.key)
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

// flushWorkers sends flush request to each worker and waits for each one to acknowledge.
func (s *webhookSink) flushWorkers(done chan struct{}) error {
	for i := 0; i < len(s.eventsChans); i++ {
		// Ability to write a nil message to events channel indicates that
		// the worker has processed all other messages.
		select {
		case <-s.workerCtx.Done():
			return s.workerCtx.Err()
		case s.eventsChans[i] <- nil:
		}
	}

	select {
	case <-s.workerCtx.Done():
		return s.workerCtx.Err()
	case done <- struct{}{}:
		return nil
	}
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
			flushRequested := msg.flushDone != nil

			if !flushRequested {
				batchTracker.addToBuffer(msg.payload)
			}

			if s.shouldSendBatch(batchTracker) || flushRequested {
				if err := s.splitAndSendBatch(batchTracker.buffer); err != nil {
					s.exitWorkersWithError(err)
					return
				}
				batchTracker.reset()

				if flushRequested {
					if err := s.flushWorkers(*msg.flushDone); err != nil {
						s.exitWorkersWithError(err)
						return
					}
				}
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
				if err := s.splitAndSendBatch(batchTracker.buffer); err != nil {
					s.exitWorkersWithError(err)
					return
				}
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
			if msgs == nil {
				// It's a flush request: if we read it, it means all outstanding
				// requests for this worker have been completed.
				continue
			}

			var encoded encodedPayload
			var err error
			switch s.format {
			case changefeedbase.OptFormatJSON:
				encoded, err = encodePayloadJSONWebhook(msgs)
			case changefeedbase.OptFormatCSV:
				encoded, err = encodePayloadCSVWebhook(msgs)
			}
			if err != nil {
				s.exitWorkersWithError(err)
				return
			}
			if err := s.sendMessageWithRetries(s.workerCtx, encoded.data); err != nil {
				s.exitWorkersWithError(err)
				return
			}
			encoded.alloc.Release(s.workerCtx)
			s.metrics.recordEmittedBatch(
				encoded.emitTime, len(msgs), encoded.mvcc, len(encoded.data), sinkDoesNotCompress)
		}
	}
}

func (s *webhookSink) sendMessageWithRetries(ctx context.Context, reqBody []byte) error {
	requestFunc := func() error {
		return s.sendMessage(ctx, reqBody)
	}
	return retry.WithMaxAttempts(ctx, s.retryCfg, s.retryCfg.MaxRetries+1, requestFunc)
}

func (s *webhookSink) sendMessage(ctx context.Context, reqBody []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url.String(), bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	switch s.format {
	case changefeedbase.OptFormatJSON:
		req.Header.Set("Content-Type", applicationTypeJSON)
	case changefeedbase.OptFormatCSV:
		req.Header.Set("Content-Type", applicationTypeCSV)
	}

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
		resBody, err := io.ReadAll(res.Body)
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

// exitWorkersWithError saves the first error message encountered by webhook workers,
// and requests all workers to terminate.
func (s *webhookSink) exitWorkersWithError(err error) {
	// errChan has buffer size 1, first error will be saved to the buffer and
	// subsequent errors will be ignored
	select {
	case s.errChan <- err:
		s.exitWorkers()
	default:
	}
}

// sinkError checks to see if any errors occurred inside workers go routines.
func (s *webhookSink) sinkError() error {
	select {
	case err := <-s.errChan:
		return err
	default:
		return nil
	}
}

func (s *webhookSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	select {
	// check the webhook sink context in case workers have been terminated
	case <-s.workerCtx.Done():
		// check again for error in case it triggered since last check
		// will return more verbose error instead of "context canceled"
		return errors.CombineErrors(s.workerCtx.Err(), s.sinkError())
	case <-ctx.Done():
		return ctx.Err()
	case err := <-s.errChan:
		return err
	case s.batchChan <- webhookMessage{
		payload: messagePayload{
			key:      key,
			val:      value,
			alloc:    alloc,
			emitTime: timeutil.Now(),
			mvcc:     mvcc,
		}}:
		s.metrics.recordMessageSize(int64(len(key) + len(value)))
	}
	return nil
}

func (s *webhookSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	defer s.metrics.recordResolvedCallback()()

	payload, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
	if err != nil {
		return err
	}

	select {
	// check the webhook sink context in case workers have been terminated
	case <-s.workerCtx.Done():
		return s.workerCtx.Err()
	// non-blocking check for error, restart changefeed if encountered
	case <-s.errChan:
		return err
	default:
	}

	// do worker logic directly here instead (there's no point using workers for
	// resolved timestamps since there are no keys and everything must be
	// in order)
	if err := s.sendMessageWithRetries(ctx, payload); err != nil {
		s.exitWorkersWithError(err)
		return err
	}

	return nil
}

func (s *webhookSink) Flush(ctx context.Context) error {
	s.metrics.recordFlushRequestCallback()()

	// Send flush request.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-s.errChan:
		return err
	case s.batchChan <- webhookMessage{flushDone: &s.flushDone}:
	}

	// Wait for flush completion -- or an error.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-s.errChan:
		return err
	case <-s.flushDone:
		return s.sinkError()
	}
}

func (s *webhookSink) Close() error {
	s.exitWorkers()
	// ignore errors here since we're closing the sink anyway
	_ = s.workerGroup.Wait()
	close(s.batchChan)
	close(s.errChan)
	for _, eventsChan := range s.eventsChans {
		close(eventsChan)
	}
	s.client.CloseIdleConnections()
	return nil
}
