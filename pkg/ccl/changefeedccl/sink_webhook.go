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
	"sync"
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
	parallelism int
	client      *httputil.Client
	workerGroup ctxgroup.Group
	cancelFunc  func()
	eventsChans []chan []byte
	in          *inflightTracker
}

func makeWebhookSink(
	ctx context.Context, u sinkURL, opts map[string]string, parallelism int,
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
		ctx:         ctx,
		authHeader:  opts[changefeedbase.OptWebhookAuthHeader],
		cancelFunc:  cancel,
		parallelism: parallelism,
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

	sink.in = makeInflightTracker()
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

// defaultWorkerCount() is the number of CPU's on the machine
func defaultWorkerCount() int {
	return system.NumCPU()
}

func (s *webhookSink) setupWorkers() {
	s.eventsChans = make([]chan []byte, s.parallelism)
	s.workerGroup = ctxgroup.WithContext(s.ctx)
	for i := 0; i < s.parallelism; i++ {
		s.eventsChans[i] = make(chan []byte)
		j := i
		s.workerGroup.GoCtx(func(ctx context.Context) error {
			s.workerLoop(s.eventsChans[j])
			return nil
		})
	}
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
			s.in.maybeSetError(err)
			// reduce inflight count by one
			s.in.Done()
		}
	}
}

func (s *webhookSink) Dial() error {
	s.setupWorkers()
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
func (s *webhookSink) workerIndex(key []byte) uint32 {
	return crc32.ChecksumIEEE(key) % uint32(s.parallelism)
}

// TODO (ryan min): add memory monitoring for inflight messages
// inflightTracker wraps logic for counting number of inflight messages to
// track when flushing sink, with error handling functionality. Implemented
// as a wrapper for WaitGroup under a lock to block additional messages while
// flushing.
type inflightTracker struct {
	// two mutexes are needed here since different goroutines will be accessing
	// both the waitgroup and the flushErr at the same time. (i.e. when waiting,
	// before Done() is called, the error is set, causes deadlock if the same
	// lock is used)
	wgMu struct {
		syncutil.Mutex
		inflightGroup *sync.WaitGroup
	}
	errMu struct {
		syncutil.Mutex
		flushErr error
	}
}

func makeInflightTracker() *inflightTracker {
	inflight := &inflightTracker{}
	inflight.wgMu.inflightGroup = new(sync.WaitGroup)
	return inflight
}

// maybeSetError sets flushErr to be err if it has not already been set.
func (i *inflightTracker) maybeSetError(err error) {
	i.errMu.Lock()
	defer i.errMu.Unlock()
	if err == nil || i.errMu.flushErr != nil {
		return
	}
	i.errMu.flushErr = err
}

// Add enqueues one inflight message to be flushed.
func (i *inflightTracker) Add() {
	i.wgMu.Lock()
	defer i.wgMu.Unlock()
	i.wgMu.inflightGroup.Add(1)
}

// Done tells the inflight tracker one message has been delivered.
func (i *inflightTracker) Done() {
	i.wgMu.inflightGroup.Done()
}

// Wait waits for all inflight messages to be delivered (inflight = 0) and
// returns a possible error
func (i *inflightTracker) Wait() error {
	i.wgMu.Lock()
	defer i.wgMu.Unlock()
	defer i.errMu.Unlock()
	i.wgMu.inflightGroup.Wait()
	// lock here to avoid deadlock with wgMu
	i.errMu.Lock()
	err := i.errMu.flushErr
	i.errMu.flushErr = nil
	return err
}

func (s *webhookSink) EmitRow(
	ctx context.Context, _ TopicDescriptor, key, value []byte, _ hlc.Timestamp,
) error {
	index := s.workerIndex(key)
	s.in.Add()

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
	s.in.Add()

	err = s.sendMessage(ctx, j)
	s.in.maybeSetError(err)
	s.in.Done()
	return nil
}

func (s *webhookSink) Flush(ctx context.Context) error {
	return s.in.Wait()
}

func (s *webhookSink) Close() error {
	// ignore errors here since we're closing the sink anyway
	_ = s.Flush(s.ctx)
	s.cancelFunc()
	_ = s.workerGroup.Wait()
	for i := 0; i < len(s.eventsChans); i++ {
		close(s.eventsChans[i])
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
