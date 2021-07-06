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
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/system"
	"github.com/cockroachdb/errors"
)

const (
	applicationTypeJSON = `application/json`
	authorizationHeader = `Authorization`
	defaultConnTimeout  = 3 * time.Second
)

var numWorkers = workerCount()

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
}

func encodePayloadWebhook(value []byte) ([]byte, error) {
	payload := json.RawMessage(value)
	// the 'payload' field has an array as a value to support
	// batched rows in the future
	body := &webhookSinkPayload{
		Payload: []json.RawMessage{payload},
	}
	j, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return j, err
}

type webhookSink struct {
	ctx         context.Context
	url         sinkURL
	authHeader  string
	client      *httputil.Client
	workerGroup ctxgroup.Group
	cancelFunc  func()
	eventsChans []chan []byte
	resultsChan chan error
	mu          struct {
		syncutil.Mutex
		flushChan chan struct{}
		flushErr  error
		inflight  int
	}
}

func makeWebhookSink(ctx context.Context, u sinkURL, opts map[string]string) (Sink, error) {
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
		ctx:        ctx,
		authHeader: opts[changefeedbase.OptWebhookAuthHeader],
		cancelFunc: cancel,
	}

	var err error
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

	sink.setupWorkers()
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

// workerCount() is the number of CPU's on the machine
func workerCount() int {
	return system.NumCPU()
}

func (s *webhookSink) setupWorkers() {
	s.eventsChans = make([]chan []byte, numWorkers)
	s.resultsChan = make(chan error)
	s.workerGroup = ctxgroup.WithContext(s.ctx)
	for i := 0; i < numWorkers; i++ {
		s.eventsChans[i] = make(chan []byte)
		j := i
		s.workerGroup.GoCtx(func(ctx context.Context) error {
			s.workerLoop(s.eventsChans[j])
			return nil
		})
	}
	s.workerGroup.GoCtx(func(ctx context.Context) error {
		s.processResultsLoop()
		return nil
	})
}

func (s *webhookSink) workerLoop(workerCh chan []byte) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case value := <-workerCh:
			msg, err := encodePayloadWebhook(value)
			if err == nil {
				err = s.sendMessage(s.ctx, msg)
			}
			s.resultsChan <- err
		}
	}
}

func (s *webhookSink) processResultsLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case err := <-s.resultsChan:
			s.mu.Lock()
			if err != nil && s.mu.flushErr == nil {
				s.mu.flushErr = err
			}
			s.mu.inflight--
			if s.mu.inflight == 0 && s.mu.flushChan != nil {
				s.mu.flushChan <- struct{}{}
				s.mu.flushChan = nil
			}
			s.mu.Unlock()
		}
	}
}

// Dial is a no-op for this sink since we don't necessarily have
// a "health check" endpoint to use.
func (s *webhookSink) Dial() error {
	return nil
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
func workerIndex(key []byte) uint32 {
	return crc32.ChecksumIEEE(key) % uint32(numWorkers)
}

func (s *webhookSink) EmitRow(
	ctx context.Context, _ TopicDescriptor, key, value []byte, _ hlc.Timestamp,
) error {
	index := workerIndex(key)
	s.mu.Lock()
	s.mu.inflight++
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.eventsChans[index] <- value:
	}
	return nil
}

func (s *webhookSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	j, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.mu.inflight++
	s.mu.Unlock()

	err = s.sendMessage(ctx, j)
	s.resultsChan <- err
	return nil
}

func (s *webhookSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	if s.mu.inflight == 0 {
		defer s.mu.Unlock()
		err := s.mu.flushErr
		s.mu.flushErr = nil
		return err
	}
	flushCh := make(chan struct{}, 1)
	s.mu.flushChan = flushCh
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-flushCh:
		s.mu.Lock()
		err := s.mu.flushErr
		s.mu.flushErr = nil
		s.mu.Unlock()
		return err
	}
}

func (s *webhookSink) Close() error {
	// ignore errors here since we're closing the sink anyway
	_ = s.Flush(s.ctx)
	s.cancelFunc()
	_ = s.workerGroup.Wait()
	for i := 0; i < len(s.eventsChans); i++ {
		close(s.eventsChans[i])
	}
	s.mu.Lock()
	if s.mu.flushChan != nil {
		close(s.mu.flushChan)
	}
	s.mu.Unlock()
	close(s.resultsChan)
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
