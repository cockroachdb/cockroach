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
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	payload *batch

	flushGroup       ctxgroup.Group
	asyncFlushCh     chan webhookFlushRequest // channel for submitting flush requests.
	asyncFlushTermCh chan struct{}            // channel closed by async flusher to indicate an error
	asyncFlushErr    error                    // set by async flusher, prior to closing asyncFlushTermCh

	// Webhook configuration.
	retryCfg retry.Options
	batchCfg batchConfig
	ts       timeutil.TimeSource
	format   changefeedbase.FormatType

	// Webhook destination.
	url        sinkURL
	authHeader string
	client     *httputil.Client

	metrics metricsRecorder
	// 	parallelism int

	//// messages are written onto batch channel
	//// which batches matches based on batching configuration.
	//batchChan chan webhookMessage
	//
	//// flushDone channel signaled when flushing completes.
	//flushDone chan struct{}
	//
	//// errChan is written to indicate an error while sending message.
	//errChan chan error
	//
	//// parallelism workers are created and controlled by the workerGroup, running with workerCtx.
	//// each worker gets its own events channel.
	//workerCtx   context.Context
	//workerGroup ctxgroup.Group
	//exitWorkers func() // Signaled to shut down all workers.
	//eventsChans []chan []messagePayload
	//metrics     metricsRecorder
}

func (s *webhookSink) getConcreteType() sinkType {
	return sinkTypeWebhook
}

type webhookSinkPayload struct {
	Payload []json.RawMessage `json:"payload"`
	Length  int               `json:"length"`
}

type batch struct {
	data      []json.RawMessage
	buffered  int
	alloc     kvevent.Alloc
	batchTime time.Time
	mvcc      hlc.Timestamp
}
type webhookFlushRequest struct {
	payload *batch
	flush   chan struct{}
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

	encodingOpts.TopicInValue = true

	if encodingOpts.Envelope != changefeedbase.OptEnvelopeBare {
		encodingOpts.KeyInValue = true
	}

	var connTimeout time.Duration
	if opts.ClientTimeout != nil {
		connTimeout = *opts.ClientTimeout
	}

	sink := &webhookSink{
		authHeader: opts.AuthHeader,
		ts:         source,
		metrics:    mb(requiresResourceAccounting),
		format:     encodingOpts.Format,

		payload: &batch{},
		// TODO (yevgeniy): Consider adding ctx to Dial method instead.
		flushGroup:       ctxgroup.WithContext(ctx),
		asyncFlushCh:     make(chan webhookFlushRequest, flushQueueDepth),
		asyncFlushTermCh: make(chan struct{}),
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
	s.flushGroup.GoCtx(s.asyncFlusher)
	return nil
}

func (s *webhookSink) shouldSendBatch() bool {
	// similar to sarama, send batch if:
	// everything is zero (default)
	// any one of the conditions are met UNLESS the condition is zero which means never batch
	switch {
	// all zero values should batch every time, otherwise batch will wait forever.
	case len(s.payload.data) > 0 &&
		s.batchCfg.Messages == 0 && s.batchCfg.Bytes == 0 && s.batchCfg.Frequency == 0:
		return true
	// messages threshold has been reached.
	case s.batchCfg.Messages > 0 && len(s.payload.data) >= s.batchCfg.Messages:
		return true
	// bytes threshold has been reached.
	case s.batchCfg.Bytes > 0 && s.payload.buffered >= s.batchCfg.Bytes:
		return true
		// frequency threshold has been reached.
	case len(s.payload.data) > 0 &&
		s.batchCfg.Frequency > 0 &&
		timeutil.Since(s.payload.batchTime) > time.Duration(s.batchCfg.Frequency):
		return true
	default:
		return false
	}
}

func (s *webhookSink) TryFlush(ctx context.Context) error {
	if !s.shouldSendBatch() {
		return nil
	}
	return s.doFlush(ctx)
}

func (s *webhookSink) doFlush(ctx context.Context) error {
	req := webhookFlushRequest{payload: s.payload}
	s.payload = &batch{}

	// Try to submit flush request, but produce warning message
	// if we can't.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.asyncFlushTermCh:
		return s.asyncFlushErr
	case s.asyncFlushCh <- req:
		return nil
	default:
		if logQueueDepth.ShouldLog() {
			log.Info(ctx, "webhook flush queue is full")
		}
	}

	// Queue was full, block until it's not.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.asyncFlushTermCh:
		return s.asyncFlushErr
	case s.asyncFlushCh <- req:
		return nil
	}
}

func (s *webhookSink) sendMessageWithRetries(ctx context.Context, reqBody []byte) error {
	requestFunc := func() error {
		return s.sendMessage(ctx, reqBody)
	}
	return retry.WithMaxAttempts(ctx, s.retryCfg, s.retryCfg.MaxRetries+1, requestFunc)
}

func (s *webhookSink) sendMessage(ctx context.Context, reqBody []byte) (retErr error) {
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
	defer func() {
		retErr = errors.CombineErrors(retErr, res.Body.Close())
	}()

	if !(res.StatusCode >= http.StatusOK && res.StatusCode < http.StatusMultipleChoices) {
		resBody, err := io.ReadAll(res.Body)
		if err != nil {
			return errors.Wrapf(err, "failed to read body for HTTP response with status: %d", res.StatusCode)
		}
		return fmt.Errorf("%s: %s", res.Status, string(resBody))
	}
	return nil
}

func (s *webhookSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	s.payload.data = append(s.payload.data, value)
	s.payload.alloc.Merge(&alloc)
	if s.payload.batchTime.IsZero() {
		s.payload.batchTime = timeutil.Now()
	}
	if s.payload.mvcc.IsEmpty() {
		s.payload.mvcc = mvcc
	}
	s.metrics.recordMessageSize(int64(len(key) + len(value)))
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
	return s.sendMessage(ctx, payload)
}

func (s *webhookSink) Flush(ctx context.Context) error {
	s.metrics.recordFlushRequestCallback()()
	return s.waitAsyncFlush(ctx)
}

// waitAsyncFlush waits until all async flushes complete.
func (s *webhookSink) waitAsyncFlush(ctx context.Context) error {
	done := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.asyncFlushTermCh:
		return s.asyncFlushErr
	case s.asyncFlushCh <- webhookFlushRequest{flush: done}:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.asyncFlushTermCh:
		return s.asyncFlushErr
	case <-done:
		return nil
	}
}

func (s *webhookSink) asyncFlusher(ctx context.Context) error {
	defer close(s.asyncFlushTermCh)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case req, ok := <-s.asyncFlushCh:
			if !ok {
				return nil // we're done
			}

			// handle flush request.
			if req.flush != nil {
				close(req.flush)
				continue
			}

			var body []byte
			var err error
			switch s.format {
			case changefeedbase.OptFormatJSON:
				jsonPayload := &webhookSinkPayload{
					Payload: req.payload.data,
					Length:  len(req.payload.data),
				}
				body, err = json.Marshal(jsonPayload)
			case changefeedbase.OptFormatCSV:
				for _, m := range req.payload.data {
					body = append(body, m...)
				}
			}

			if err == nil {
				flushDone := s.metrics.recordFlushRequestCallback()
				err = s.sendMessageWithRetries(ctx, body)
				flushDone()
			}

			if err != nil {
				log.Errorf(ctx, "error flushing file to storage: %s", err)
				s.asyncFlushErr = err
				return err
			}
		}
	}
}

func (s *webhookSink) Close() error {
	if s.asyncFlushCh == nil {
		return nil
	}
	s.client.CloseIdleConnections()
	err := s.waitAsyncFlush(context.Background())
	close(s.asyncFlushCh) // signal flusher to exit.
	s.asyncFlushCh = nil
	err = errors.CombineErrors(err, s.flushGroup.Wait())
	return nil
}
