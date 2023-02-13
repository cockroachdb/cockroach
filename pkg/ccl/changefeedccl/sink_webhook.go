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
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
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

// The webhook sink is a single threaded emitter that is meant to be used as a
// single SinkWorker.
func makeWebhookSink(
	ctx context.Context,
	u sinkURL,
	encodingOpts changefeedbase.EncodingOptions,
	opts changefeedbase.WebhookSinkOptions,
	numWorkers int64,
	source timeutil.TimeSource,
	mb metricsRecorderBuilder,
	pacer SinkPacer,
) (Sink, error) {
	sinkClient, err := makeWebhookSinkClient(ctx, u, encodingOpts, opts)
	if err != nil {
		return nil, err
	}

	flushCfg, retryOpts, err := getSinkConfigFromJson(opts.JSONConfig, sinkJSONConfig{})
	if err != nil {
		return nil, err
	}

	return makeParallelBatchingSink(
		ctx,
		sinkTypeWebhook,
		sinkClient,
		flushCfg,
		retryOpts,
		numWorkers,
		nil,
		source,
		mb(requiresResourceAccounting),
		pacer,
	), nil
}

type webhookSinkClient struct {
	ctx        context.Context
	format     changefeedbase.FormatType
	url        sinkURL
	authHeader string
	client     *httputil.Client
}

var _ SinkClient = (*webhookSinkClient)(nil)

func makeWebhookSinkClient(
	ctx context.Context,
	u sinkURL,
	encodingOpts changefeedbase.EncodingOptions,
	opts changefeedbase.WebhookSinkOptions,
) (SinkClient, error) {
	err := validateWebhookOpts(u, encodingOpts, opts)
	if err != nil {
		return nil, err
	}

	u.Scheme = strings.TrimPrefix(u.Scheme, `webhook-`)

	sinkClient := &webhookSinkClient{
		ctx:        ctx,
		authHeader: opts.AuthHeader,
		format:     encodingOpts.Format,
	}

	var connTimeout time.Duration
	if opts.ClientTimeout != nil {
		connTimeout = *opts.ClientTimeout
	}
	sinkClient.client, err = makeWebhookClient(u, connTimeout)
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
	sinkClient.url = sinkURL{URL: sinkURLParsed}

	return sinkClient, nil
}

func makeWebhookClient(u sinkURL, timeout time.Duration) (*httputil.Client, error) {
	defaultTransport := http.DefaultTransport.(*http.Transport)
	client := &httputil.Client{
		Client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				DialContext:         (&net.Dialer{Timeout: timeout}).DialContext,
				IdleConnTimeout:     defaultTransport.IdleConnTimeout,
				TLSHandshakeTimeout: defaultTransport.TLSHandshakeTimeout,

				// All connections will likely be to the same host.
				MaxIdleConnsPerHost: 16,
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

func (we *webhookSinkClient) makePayloadForBytes(body []byte) (SinkPayload, error) {
	req, err := http.NewRequestWithContext(we.ctx, http.MethodPost, we.url.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	switch we.format {
	case changefeedbase.OptFormatJSON:
		req.Header.Set("Content-Type", applicationTypeJSON)
	case changefeedbase.OptFormatCSV:
		req.Header.Set("Content-Type", applicationTypeCSV)
	}

	if we.authHeader != "" {
		req.Header.Set(authorizationHeader, we.authHeader)
	}

	return req, nil
}

// EncodeBatch implements the SinkClient interface
func (we *webhookSinkClient) EncodeBatch(_ string, batch []messagePayload) (SinkPayload, error) {
	var reqBody []byte
	var err error

	switch we.format {
	case changefeedbase.OptFormatJSON:
		reqBody, err = encodeWebhookMsgJSON(batch)
	case changefeedbase.OptFormatCSV:
		reqBody, err = encodeWebhookMsgCSV(batch)
	}

	if err != nil {
		return nil, err
	}

	return we.makePayloadForBytes(reqBody)
}

type webhookJsonEvent struct {
	Payload []json.RawMessage `json:"payload"`
	Length  int               `json:"length"`
}

func encodeWebhookMsgJSON(messages []messagePayload) ([]byte, error) {
	payload := make([]json.RawMessage, len(messages))
	for i, m := range messages {
		payload[i] = m.val
	}

	body := &webhookJsonEvent{
		Payload: payload,
		Length:  len(payload),
	}
	j, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return j, err
}

func encodeWebhookMsgCSV(messages []messagePayload) ([]byte, error) {
	var mergedMsgs []byte
	for _, m := range messages {
		mergedMsgs = append(mergedMsgs, m.val...)
	}
	return mergedMsgs, nil
}

// EncodeResolvedMeessage implements the SinkClient interface
func (we *webhookSinkClient) EncodeResolvedMessage(
	payload resolvedMessagePayload,
) (SinkPayload, error) {
	return we.makePayloadForBytes(payload.body)
}

// EmitPayload implements the SinkClient interface
func (we *webhookSinkClient) EmitPayload(batch SinkPayload) error {
	req := batch.(*http.Request)
	res, err := we.client.Do(req)
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

// Close implements the SinkClient interface
func (we *webhookSinkClient) Close() error {
	we.client.CloseIdleConnections()
	return nil
}

func validateWebhookOpts(
	u sinkURL, encodingOpts changefeedbase.EncodingOptions, opts changefeedbase.WebhookSinkOptions,
) error {
	if u.Scheme != changefeedbase.SinkSchemeWebhookHTTPS {
		return errors.Errorf(`this sink requires %s`, changefeedbase.SinkSchemeHTTPS)
	}

	switch encodingOpts.Format {
	case changefeedbase.OptFormatJSON:
	case changefeedbase.OptFormatCSV:
	default:
		return errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptFormat, encodingOpts.Format)
	}

	switch encodingOpts.Envelope {
	case changefeedbase.OptEnvelopeWrapped, changefeedbase.OptEnvelopeBare:
	default:
		return errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptEnvelope, encodingOpts.Envelope)
	}

	encodingOpts.TopicInValue = true

	if encodingOpts.Envelope != changefeedbase.OptEnvelopeBare {
		encodingOpts.KeyInValue = true
	}

	return nil
}
