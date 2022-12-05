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

// The webhook sink uses sinkProcessor with its SinkPayload being HTTP requests
func makeWebhookSink(
	ctx context.Context,
	u sinkURL,
	encodingOpts changefeedbase.EncodingOptions,
	opts changefeedbase.WebhookSinkOptions,
	parallelism int,
	source timeutil.TimeSource,
	mb metricsRecorderBuilder,
) (Sink, error) {
	thinSink, err := makeWebhookThinSink(ctx, u, encodingOpts, opts)
	if err != nil {
		return nil, err
	}

	cfg := sinkProcessorConfig{
		numWorkers: parallelism,
		timeSource: source,
		keyInValue: true,
	}

	cfg.flushCfg, cfg.retryOpts, err = getWebhookSinkConfig(opts.JSONConfig)
	if err != nil {
		return nil, err
	}

	sp, err := makeSinkProcessor(context.Background(), thinSink, cfg, mb)
	if err != nil {
		return nil, err
	}
	return sp, nil
}

type webhookThinSink struct {
	ctx        context.Context
	format     changefeedbase.FormatType
	url        sinkURL
	authHeader string
	client     *httputil.Client
}

var _ ThinSink = (*webhookThinSink)(nil)

func makeWebhookThinSink(
	ctx context.Context,
	u sinkURL,
	encodingOpts changefeedbase.EncodingOptions,
	opts changefeedbase.WebhookSinkOptions,
) (ThinSink, error) {
	err := validateWebhookOpts(u, encodingOpts, opts)
	if err != nil {
		return nil, err
	}

	u.Scheme = strings.TrimPrefix(u.Scheme, `webhook-`)

	sink := &webhookThinSink{
		ctx:        ctx,
		authHeader: opts.AuthHeader,
		format:     encodingOpts.Format,
	}

	var connTimeout time.Duration
	if opts.ClientTimeout != nil {
		connTimeout = *opts.ClientTimeout
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
				DialContext:     (&net.Dialer{Timeout: timeout}).DialContext,
				IdleConnTimeout: 90 * time.Second, // taken from DefaultTransport

				// Raising this value to 200 makes little difference while reducing it
				// to 50 results in an ~8% reduction in throughput.
				MaxIdleConnsPerHost: 100,
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

func (ws *webhookThinSink) makePayloadForBytes(body []byte) (SinkPayload, error) {
	req, err := http.NewRequestWithContext(ws.ctx, http.MethodPost, ws.url.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	switch ws.format {
	case changefeedbase.OptFormatJSON:
		req.Header.Set("Content-Type", applicationTypeJSON)
	case changefeedbase.OptFormatCSV:
		req.Header.Set("Content-Type", applicationTypeCSV)
	}

	if ws.authHeader != "" {
		req.Header.Set(authorizationHeader, ws.authHeader)
	}

	return req, nil
}

func (ws *webhookThinSink) EncodeBatch(batch []MessagePayload) (SinkPayload, error) {
	var reqBody []byte
	var err error

	switch ws.format {
	case changefeedbase.OptFormatJSON:
		reqBody, err = encodeWebhookMsgJSON(batch)
	case changefeedbase.OptFormatCSV:
		reqBody, err = encodeWebhookMsgCSV(batch)
	}

	if err != nil {
		return nil, err
	}

	return ws.makePayloadForBytes(reqBody)
}

type webhookJsonEvent struct {
	Payload []json.RawMessage `json:"payload"`
	Length  int               `json:"length"`
}

func encodeWebhookMsgJSON(messages []MessagePayload) ([]byte, error) {
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

func encodeWebhookMsgCSV(messages []MessagePayload) ([]byte, error) {
	var mergedMsgs []byte
	for _, m := range messages {
		mergedMsgs = append(mergedMsgs, m.val...)
	}
	return mergedMsgs, nil
}

func (ws *webhookThinSink) EncodeResolvedMessage(
	payload ResolvedMessagePayload,
) (SinkPayload, error) {
	return ws.makePayloadForBytes(payload.body)
}

func (ws *webhookThinSink) EmitPayload(batch SinkPayload) error {
	req := batch.(*http.Request)
	res, err := ws.client.Do(req)
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

func (ws *webhookThinSink) Close() error {
	ws.client.CloseIdleConnections()
	return nil
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

	if encodingOpts.Envelope != changefeedbase.OptEnvelopeBare && !encodingOpts.KeyInValue {
		return errors.Errorf(`this sink requires the WITH %s option`, changefeedbase.OptKeyInValue)
	}

	if !encodingOpts.TopicInValue {
		return errors.Errorf(`this sink requires the WITH %s option`, changefeedbase.OptTopicInValue)
	}

	return nil
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
