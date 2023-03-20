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
	"github.com/cockroachdb/cockroach/pkg/util/admission"
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

type webhookSinkClient struct {
	ctx        context.Context
	format     changefeedbase.FormatType
	url        sinkURL
	authHeader string
	batchCfg   sinkBatchConfig
	client     *httputil.Client
}

var _ SinkClient = (*webhookSinkClient)(nil)

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

func makeWebhookClient(u sinkURL, timeout time.Duration, parallelism int) (*httputil.Client, error) {
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

func (wse *webhookSinkClient) makePayloadForBytes(body []byte) (SinkPayload, error) {
	req, err := http.NewRequestWithContext(wse.ctx, http.MethodPost, wse.url.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	switch wse.format {
	case changefeedbase.OptFormatJSON:
		req.Header.Set("Content-Type", applicationTypeJSON)
	case changefeedbase.OptFormatCSV:
		req.Header.Set("Content-Type", applicationTypeCSV)
	}

	if wse.authHeader != "" {
		req.Header.Set(authorizationHeader, wse.authHeader)
	}

	return req, nil
}

// MakeResolvedPayload implements the SinkClient interface
func (wse *webhookSinkClient) MakeResolvedPayload(
	body []byte, topic string,
) (SinkPayload, error) {
	return wse.makePayloadForBytes(body)
}

// Flush implements the SinkClient interface
func (wse *webhookSinkClient) Flush(ctx context.Context, batch SinkPayload) error {
	req := batch.(*http.Request)
	res, err := wse.client.Do(req)
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
func (wse *webhookSinkClient) Close() error {
	wse.client.CloseIdleConnections()
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

type webhookCSVWriter struct {
	bytes        []byte
	messageCount int
	sc           *webhookSinkClient
}

var _ BatchWriter = (*webhookCSVWriter)(nil)

// AppendKV implements the BatchWriter interface
func (cw *webhookCSVWriter) AppendKV(key []byte, value []byte, topic string) {
	cw.bytes = append(cw.bytes, value...)
	cw.messageCount += 1
}

// ShouldFlush implements the BatchWriter interface
func (cw *webhookCSVWriter) ShouldFlush() bool {
	return cw.sc.shouldFlush(len(cw.bytes), cw.messageCount)
}

// Close implements the BatchWriter interface
func (cw *webhookCSVWriter) Close() (SinkPayload, error) {
	return cw.sc.makePayloadForBytes(cw.bytes)
}

type webhookJSONWriter struct {
	numMessages int
	sc          *webhookSinkClient
	buffer      bytes.Buffer
}

var _ BatchWriter = (*webhookJSONWriter)(nil)

// AppendKV implements the BatchWriter interface
func (jw *webhookJSONWriter) AppendKV(key []byte, value []byte, topic string) {
	if jw.numMessages == 0 {
		jw.buffer.WriteString("{\"payload\":[")
		jw.buffer.Write(value)
	} else {
		jw.buffer.WriteByte(',')
		jw.buffer.Write(value)
	}
	jw.numMessages += 1
}

// ShouldFlush implements the BatchWriter interface
func (jw *webhookJSONWriter) ShouldFlush() bool {
	return jw.sc.shouldFlush(len(jw.buffer.Bytes()), jw.numMessages)
}

// Close implements the BatchWriter interface
func (jw *webhookJSONWriter) Close() (SinkPayload, error) {
	jw.buffer.WriteString(fmt.Sprintf("],\"length\":%d}", jw.numMessages))
	return jw.sc.makePayloadForBytes(jw.buffer.Bytes())
}

func (wse *webhookSinkClient) MakeBatchWriter() BatchWriter {
	if wse.format == changefeedbase.OptFormatCSV {
		return &webhookCSVWriter{sc: wse}
	} else {
		return &webhookJSONWriter{
			sc: wse,
		}
	}
}

func (wse *webhookSinkClient) shouldFlush(bytes int, messages int) bool {
	switch {
	// all zero values is interpreted as flush every time
	case wse.batchCfg.Messages == 0 && wse.batchCfg.Bytes == 0 && wse.batchCfg.Frequency == 0:
		return true
	// messages threshold has been reached
	case wse.batchCfg.Messages > 0 && messages >= wse.batchCfg.Messages:
		return true
	// bytes threshold has been reached
	case wse.batchCfg.Bytes > 0 && bytes >= wse.batchCfg.Bytes:
		return true
	default:
		return false
	}
}

func makeWebhookSink(
	ctx context.Context,
	u sinkURL,
	encodingOpts changefeedbase.EncodingOptions,
	opts changefeedbase.WebhookSinkOptions,
	parallelism int,
	pacer *admission.Pacer,
	source timeutil.TimeSource,
	mb metricsRecorderBuilder,
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
		pacer,
		source,
		mb(requiresResourceAccounting),
	), nil
}
