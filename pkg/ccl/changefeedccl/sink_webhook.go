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
	"hash"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
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
	hasher  hash.Hash32
	io      *asyncIO

	// Webhook configuration.
	sendMessage httpPostFn
	batchCfg    batchConfig
	ts          timeutil.TimeSource

	metrics metricsRecorder
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
	keys      intsets.Fast
	buffered  int
	alloc     kvevent.Alloc
	batchTime time.Time
	mvcc      hlc.Timestamp
}

func (b *batch) Keys() intsets.Fast {
	return b.keys
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

func getWebhookSinkConfig(
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

	// TODO(yevgeniy): Establish HTTP connection in Dial().
	httpClient, err := makeWebhookClient(&u, connTimeout)
	if err != nil {
		return nil, err
	}

	batchCfg, retryCfg, err := getWebhookSinkConfig(opts.JSONConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "error processing option %s", changefeedbase.OptWebhookSinkConfig)
	}

	metrics := mb(requiresResourceAccounting)
	httpPost := getMessageSender(httpClient, u.String(), opts.AuthHeader, encodingOpts.Format, retryCfg)
	httpHandler := getBatchSender(encodingOpts.Format, httpPost, metrics)
	sink := &webhookSink{
		ts:          source,
		batchCfg:    batchCfg,
		sendMessage: httpPost,
		metrics:     metrics,
		payload:     &batch{},
		hasher:      makeHasher(),
		io:          newAsyncIO(httpHandler),
	}
	sink.io.Start(ctx, 32)
	return sink, nil
}

func makeWebhookClient(u *sinkURL, timeout time.Duration) (*httputil.Client, error) {
	client := &httputil.Client{
		Client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxConnsPerHost:     100, // This should probably be == to parallelism.
				MaxIdleConnsPerHost: 100,
				MaxIdleConns:        100,
				IdleConnTimeout:     time.Minute,
				ForceAttemptHTTP2:   true,
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
		MaxRetries:     10,
	}
	return opts
}

// defaultWorkerCount() is the number of CPU's on the machine
func defaultWorkerCount() int {
	return system.NumCPU()
}

func (s *webhookSink) Dial() error {
	return nil
}

func (s *webhookSink) shouldFlush() bool {
	// similar to sarama, send batch if:
	// everything is zero (default)
	// any one of the conditions are met UNLESS the condition is zero which means never batch
	switch {
	// all zero values should batch every time, otherwise batch will wait forever.
	case s.batchCfg.Messages == 0 && s.batchCfg.Bytes == 0 && s.batchCfg.Frequency == 0:
		return true
	// messages threshold has been reached.
	case s.batchCfg.Messages > 0 && len(s.payload.data) >= s.batchCfg.Messages:
		return true
	// bytes threshold has been reached.
	case s.batchCfg.Bytes > 0 && s.payload.buffered >= s.batchCfg.Bytes:
		return true
		// frequency threshold has been reached.
	case s.batchCfg.Frequency > 0 &&
		timeutil.Since(s.payload.batchTime) > time.Duration(s.batchCfg.Frequency):
		return true
	default:
		return false
	}
}

func (s *webhookSink) TryFlush(ctx context.Context) error {
	if s.payload.buffered == 0 || !s.shouldFlush() {
		return nil
	}
	return s.flushBatch(ctx)
}

func (s *webhookSink) flushBatch(ctx context.Context) error {
	req := s.payload
	s.payload = &batch{}

	// Try to submit flush request, but produce warning message
	// if we can't.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.io.Done:
		return s.io.Err()
	case s.io.Do <- req:
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
	case <-s.io.Done:
		return s.io.Err()
	case s.io.Do <- req:
		return nil
	}
}

type httpPostFn func(ctx context.Context, body []byte) error

// getMessageSender returns a function that can be used to post content to the
// provided destination URL, via specified client. Message is formatted as per
// specified message format, and errors are retried as per specified retry
// config.
func getMessageSender(
	client *httputil.Client,
	destURL string,
	authHeader string,
	msgFormat changefeedbase.FormatType,
	retryCfg retry.Options,
) httpPostFn {
	issueRequest := func(ctx context.Context, body []byte) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, destURL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		switch msgFormat {
		case changefeedbase.OptFormatJSON:
			req.Header.Set("Content-Type", applicationTypeJSON)
		case changefeedbase.OptFormatCSV:
			req.Header.Set("Content-Type", applicationTypeCSV)
		}

		if authHeader != "" {
			req.Header.Set(authorizationHeader, authHeader)
		}

		var res *http.Response
		res, err = client.Do(req)
		if err != nil {
			return err
		}

		if !(res.StatusCode >= http.StatusOK && res.StatusCode < http.StatusMultipleChoices) {
			resBody, err := io.ReadAll(res.Body)
			if err != nil {
				return errors.Wrapf(err, "failed to read body for HTTP response with status: %d", res.StatusCode)
			}
			return errors.Newf("%s: %s", res.Status, string(resBody))
		}
		return nil
	}

	return func(ctx context.Context, body []byte) error {
		return retry.WithMaxAttempts(ctx, retryCfg, retryCfg.MaxRetries+1, func() error {
			return issueRequest(ctx, body)
		})
	}
}

// getBatchSender returns IOHandler to send batches of CDC messages using provided httpPostFn.
func getBatchSender(
	msgFormat changefeedbase.FormatType, sendMsg httpPostFn, metrics metricsRecorder,
) IOHandler {
	return func(ctx context.Context, b Batch) error {
		payload, ok := b.(*batch)
		if !ok {
			return errors.AssertionFailedf("expected webhook batch, found %T", b)
		}

		var buf bytes.Buffer
		switch msgFormat {
		case changefeedbase.OptFormatJSON:
			jsonPayload := &webhookSinkPayload{
				Payload: payload.data,
				Length:  len(payload.data),
			}
			var err error
			body, err := json.Marshal(jsonPayload)
			if err != nil {
				return err
			}
			buf.Write(body)
		case changefeedbase.OptFormatCSV:
			for _, m := range payload.data {
				buf.Write(m)
			}
		}

		defer func() {
			metrics.recordEmittedBatch(payload.batchTime, int(payload.alloc.Events()),
				payload.mvcc, buf.Len(), buf.Len())
			payload.alloc.Release(ctx)
		}()
		return sendMsg(ctx, buf.Bytes())
	}
}

func hash32(h hash.Hash32, buf []byte) uint32 {
	h.Reset()
	h.Write(buf)
	return h.Sum32()
}

func (s *webhookSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	s.payload.data = append(s.payload.data, value)
	s.payload.keys.Add(int(hash32(s.hasher, key)))
	s.payload.buffered += len(value)
	s.payload.alloc.Merge(&alloc)
	if s.payload.batchTime.IsZero() {
		s.payload.batchTime = timeutil.Now()
	}
	if s.payload.mvcc.IsEmpty() {
		s.payload.mvcc = mvcc
	}
	s.metrics.recordMessageSize(int64(len(key) + len(value)))
	return s.TryFlush(ctx)
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
	defer s.metrics.recordFlushRequestCallback()()
	if s.payload.buffered == 0 {
		return nil
	}

	return s.io.Flush(ctx)
}

func (s *webhookSink) Close() error {
	return s.io.Close()
}
