// Copyright 2023 The Cockroach Authors.
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
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
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

type webhookSinkClient struct {
	ctx        context.Context
	format     changefeedbase.FormatType
	url        sinkURL
	authHeader string
	batchCfg   sinkBatchConfig
	client     *httputil.Client
}

var _ SinkClient = (*webhookSinkClient)(nil)
var _ SinkPayload = (*http.Request)(nil)

func makeWebhookSinkClient(
	ctx context.Context,
	u sinkURL,
	encodingOpts changefeedbase.EncodingOptions,
	opts changefeedbase.WebhookSinkOptions,
	batchCfg sinkBatchConfig,
	parallelism int,
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
		batchCfg:   batchCfg,
	}

	var connTimeout time.Duration
	if opts.ClientTimeout != nil {
		connTimeout = *opts.ClientTimeout
	}
	sinkClient.client, err = makeWebhookClient(u, connTimeout, parallelism)
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

func makeWebhookClient(
	u sinkURL, timeout time.Duration, parallelism int,
) (*httputil.Client, error) {
	client := &httputil.Client{
		Client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				DialContext:         (&net.Dialer{Timeout: timeout}).DialContext,
				MaxConnsPerHost:     parallelism,
				MaxIdleConnsPerHost: parallelism,
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

func (sc *webhookSinkClient) makePayloadForBytes(body []byte) (SinkPayload, error) {
	req, err := http.NewRequestWithContext(sc.ctx, http.MethodPost, sc.url.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	switch sc.format {
	case changefeedbase.OptFormatJSON:
		req.Header.Set("Content-Type", applicationTypeJSON)
	case changefeedbase.OptFormatCSV:
		req.Header.Set("Content-Type", applicationTypeCSV)
	}

	if sc.authHeader != "" {
		req.Header.Set(authorizationHeader, sc.authHeader)
	}

	return req, nil
}

// FlushResolvedPayload implements the SinkClient interface
func (sc *webhookSinkClient) FlushResolvedPayload(
	ctx context.Context, body []byte, _ func(func(topic string) error) error, retryOpts retry.Options,
) error {
	pl, err := sc.makePayloadForBytes(body)
	if err != nil {
		return err
	}
	return retry.WithMaxAttempts(ctx, retryOpts, retryOpts.MaxRetries+1, func() error {
		return sc.Flush(ctx, pl)
	})
}

// Flush implements the SinkClient interface
func (sc *webhookSinkClient) Flush(ctx context.Context, batch SinkPayload) error {
	req := batch.(*http.Request)
	b, err := req.GetBody()
	if err != nil {
		return err
	}
	req.Body = b
	res, err := sc.client.Do(req)
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
func (sc *webhookSinkClient) Close() error {
	sc.client.CloseIdleConnections()
	return nil
}

func (sc *webhookSinkClient) CheckConnection(ctx context.Context) error {
	return nil
}

func validateWebhookOpts(
	u sinkURL, encodingOpts changefeedbase.EncodingOptions, opts changefeedbase.WebhookSinkOptions,
) error {
	if u.Scheme != changefeedbase.SinkSchemeWebhookHTTPS {
		return errors.Errorf(`this sink requires %s`, changefeedbase.SinkSchemeWebhookHTTPS)
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

type webhookCSVBuffer struct {
	bytes        []byte
	messageCount int
	sc           *webhookSinkClient
}

var _ BatchBuffer = (*webhookCSVBuffer)(nil)

// Append implements the BatchBuffer interface
func (cb *webhookCSVBuffer) Append(key []byte, value []byte, _ attributes) {
	cb.bytes = append(cb.bytes, value...)
	cb.messageCount += 1
}

// ShouldFlush implements the BatchBuffer interface
func (cb *webhookCSVBuffer) ShouldFlush() bool {
	return shouldFlushBatch(len(cb.bytes), cb.messageCount, cb.sc.batchCfg)
}

// Close implements the BatchBuffer interface
func (cb *webhookCSVBuffer) Close() (SinkPayload, error) {
	return cb.sc.makePayloadForBytes(cb.bytes)
}

type webhookJSONBuffer struct {
	messages [][]byte
	numBytes int
	sc       *webhookSinkClient
}

var _ BatchBuffer = (*webhookJSONBuffer)(nil)

// Append implements the BatchBuffer interface
func (jb *webhookJSONBuffer) Append(key []byte, value []byte, _ attributes) {
	jb.messages = append(jb.messages, value)
	jb.numBytes += len(value)
}

// ShouldFlush implements the BatchBuffer interface
func (jb *webhookJSONBuffer) ShouldFlush() bool {
	return shouldFlushBatch(jb.numBytes, len(jb.messages), jb.sc.batchCfg)
}

// Close implements the BatchBuffer interface
func (jb *webhookJSONBuffer) Close() (SinkPayload, error) {
	var buffer bytes.Buffer
	prefix := "{\"payload\":["
	suffix := fmt.Sprintf("],\"length\":%d}", len(jb.messages))

	// Grow all at once to avoid reallocations
	buffer.Grow(len(prefix) + jb.numBytes /* msgs */ + len(jb.messages) /* commas */ + len(suffix))

	buffer.WriteString(prefix)
	for i, msg := range jb.messages {
		if i != 0 {
			buffer.WriteByte(',')
		}
		buffer.Write(msg)
	}
	buffer.WriteString(suffix)
	return jb.sc.makePayloadForBytes(buffer.Bytes())
}

// MakeBatchBuffer implements the SinkClient interface
func (sc *webhookSinkClient) MakeBatchBuffer(topic string) BatchBuffer {
	if sc.format == changefeedbase.OptFormatCSV {
		return &webhookCSVBuffer{sc: sc}
	} else {
		return &webhookJSONBuffer{
			sc:       sc,
			messages: make([][]byte, 0, sc.batchCfg.Messages),
		}
	}
}

func makeWebhookSink(
	ctx context.Context,
	u sinkURL,
	encodingOpts changefeedbase.EncodingOptions,
	opts changefeedbase.WebhookSinkOptions,
	parallelism int,
	pacerFactory func() *admission.Pacer,
	source timeutil.TimeSource,
	mb metricsRecorderBuilder,
	settings *cluster.Settings,
) (Sink, error) {
	batchCfg, retryOpts, err := getSinkConfigFromJson(opts.JSONConfig, sinkJSONConfig{})
	if err != nil {
		return nil, err
	}

	sinkClient, err := makeWebhookSinkClient(ctx, u, encodingOpts, opts, batchCfg, parallelism)
	if err != nil {
		return nil, err
	}

	return makeBatchingSink(
		ctx,
		sinkTypeWebhook,
		sinkClient,
		time.Duration(batchCfg.Frequency),
		retryOpts,
		parallelism,
		nil,
		pacerFactory,
		source,
		mb(requiresResourceAccounting),
		settings,
	), nil
}
