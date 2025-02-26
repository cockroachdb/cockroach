// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func getGenericWebhookSinkOptions(
	overrides ...struct {
		key   string
		value string
	},
) changefeedbase.StatementOptions {

	opts := make(map[string]string)
	opts[changefeedbase.OptFormat] = string(changefeedbase.OptFormatJSON)
	opts[changefeedbase.OptKeyInValue] = ``
	opts[changefeedbase.OptEnvelope] = string(changefeedbase.OptEnvelopeWrapped)
	opts[changefeedbase.OptTopicInValue] = ``
	// speed up test by using faster backoff times
	opts[changefeedbase.OptWebhookSinkConfig] = `{"Retry":{"Backoff": "5ms"}}`

	for _, o := range overrides {
		opts[o.key] = o.value
	}

	return changefeedbase.MakeStatementOptions(opts)
}

// repeatStatusCode returns an array of status codes that the mock
// webhook sink should return for subsequent requests to ensure that an error
// is reached even with the default retry behavior.
func repeatStatusCode(code int, count int) []int {
	arr := make([]int, count)
	for i := 0; i < count; i++ {
		arr[i] = code
	}
	return arr
}

func setupWebhookSinkWithDetails(
	ctx context.Context,
	details jobspb.ChangefeedDetails,
	parallelism int,
	source timeutil.TimeSource,
) (Sink, error) {
	u, err := url.Parse(details.SinkURI)
	if err != nil {
		return nil, err
	}

	opts := changefeedbase.MakeStatementOptions(details.Opts)

	encodingOpts, err := opts.GetEncodingOptions()
	if err != nil {
		return nil, err
	}
	sinkOpts, err := opts.GetWebhookSinkOptions()
	if err != nil {
		return nil, err
	}
	sinkSrc, err := makeWebhookSink(ctx, &changefeedbase.SinkURL{URL: u}, encodingOpts, sinkOpts, parallelism, nilPacerFactory, source, nilMetricsRecorderBuilder, cluster.MakeClusterSettings())
	if err != nil {
		return nil, err
	}

	if err := sinkSrc.Dial(); err != nil {
		return nil, err
	}

	return sinkSrc, nil
}

// general happy path for webhook sink
func testSendAndReceiveRows(t *testing.T, sinkSrc Sink, sinkDest *cdctest.MockWebhookSink) {
	ctx := context.Background()

	// test an insert row entry
	var pool testAllocPool
	require.NoError(t, sinkSrc.EmitRow(ctx, noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, pool.alloc()))
	require.NoError(t, sinkSrc.EmitRow(ctx, noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1002},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, pool.alloc()))
	require.NoError(t, sinkSrc.Flush(ctx))
	testutils.SucceedsSoon(t, func() error {
		remaining := pool.used()
		if remaining == 0 {
			return nil
		}
		return errors.Newf("waiting for 0 allocs (%d)", remaining)
	})

	require.Equal(t,
		`{"payload":[{"after":{"col1":"val1","rowid":1002},"key":[1001],"topic:":"foo"}],"length":1}`, sinkDest.Latest(),
		"sink %s expected to receive message %s", sinkDest.URL(),
		`{"payload":[{"after":{"col1":"val1","rowid":1002},"key":[1001],"topic:":"foo"}],"length":1}`)

	// test a delete row entry
	require.NoError(t, sinkSrc.EmitRow(ctx, noTopic{}, []byte("[1002]"), []byte(`{"after":null,"key":[1002],"topic:":"foo"}`), zeroTS, zeroTS, pool.alloc()))
	require.NoError(t, sinkSrc.Flush(ctx))

	require.Equal(t,
		`{"payload":[{"after":null,"key":[1002],"topic:":"foo"}],"length":1}`, sinkDest.Latest(),
		"sink %s expected to receive message %s", sinkDest.URL(),
		`{"payload":[{"after":null,"key":[1002],"topic:":"foo"}],"length":1}`)

	opts, err := getGenericWebhookSinkOptions().GetEncodingOptions()
	require.NoError(t, err)
	enc, err := makeJSONEncoder(ctx, jsonEncoderOptions{EncodingOptions: opts}, getTestingEnrichedSourceProvider(opts))
	require.NoError(t, err)

	// test a resolved timestamp entry
	require.NoError(t, sinkSrc.EmitResolvedTimestamp(ctx, Encoder(enc), hlc.Timestamp{WallTime: 2}))
	require.NoError(t, sinkSrc.Flush(ctx))

	require.Equal(t,
		`{"resolved":"2.0000000000"}`, sinkDest.Latest(),
		"sink %s expected to receive message %s", sinkDest.URL(),
		`{"resolved":"2.0000000000"}`)
}

func TestWebhookSink(t *testing.T) {
	defer leaktest.AfterTest(t)()

	webhookSinkTestfn := func(parallelism int, opts map[string]string) {
		cert, certEncoded, err := cdctest.NewCACertBase64Encoded()
		require.NoError(t, err)
		sinkDest, err := cdctest.StartMockWebhookSink(cert)
		require.NoError(t, err)

		sinkDestHost, err := url.Parse(sinkDest.URL())
		require.NoError(t, err)

		params := sinkDestHost.Query()
		params.Set(changefeedbase.SinkParamCACert, certEncoded)
		sinkDestHost.RawQuery = params.Encode()

		details := jobspb.ChangefeedDetails{
			SinkURI: fmt.Sprintf("webhook-%s", sinkDestHost.String()),
			Opts:    opts,
		}

		sinkSrc, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism, timeutil.DefaultTimeSource{})
		require.NoError(t, err)

		// sink with client accepting server cert should pass
		testSendAndReceiveRows(t, sinkSrc, sinkDest)

		params.Del(changefeedbase.SinkParamCACert)
		sinkDestHost.RawQuery = params.Encode()
		details.SinkURI = fmt.Sprintf("webhook-%s", sinkDestHost.String())
		sinkSrcNoCert, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism, timeutil.DefaultTimeSource{})
		require.NoError(t, err)

		// now sink's client accepts no custom certs, should reject the server's cert and fail
		require.NoError(t, sinkSrcNoCert.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, zeroAlloc))

		require.Regexp(t, "x509", sinkSrcNoCert.Flush(context.Background()))

		params.Set(changefeedbase.SinkParamSkipTLSVerify, "true")
		sinkDestHost.RawQuery = params.Encode()
		details.SinkURI = fmt.Sprintf("webhook-%s", sinkDestHost.String())
		sinkSrcInsecure, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism, timeutil.DefaultTimeSource{})
		require.NoError(t, err)

		// client should allow unrecognized certs and pass
		testSendAndReceiveRows(t, sinkSrcInsecure, sinkDest)

		// sink should throw an error if server is unreachable
		sinkDest.Close()
		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, zeroAlloc))

		err = sinkSrc.Flush(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), fmt.Sprintf(`Post "%s":`, sinkDest.URL()))

		sinkDestHTTP, err := cdctest.StartMockWebhookSinkInsecure()
		require.NoError(t, err)
		details.SinkURI = fmt.Sprintf("webhook-https://%s", strings.TrimPrefix(sinkDestHTTP.URL(), "http://"))

		sinkSrcWrongProtocol, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism, timeutil.DefaultTimeSource{})
		require.NoError(t, err)

		// sink's client should not accept the endpoint's use of HTTP (expects HTTPS)
		require.NoError(t, sinkSrcWrongProtocol.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, zeroAlloc))

		require.EqualError(t, sinkSrcWrongProtocol.Flush(context.Background()),
			fmt.Sprintf(`Post "%s": http: server gave HTTP response to HTTPS client`, fmt.Sprintf("https://%s", strings.TrimPrefix(sinkDestHTTP.URL(),
				"http://"))))

		sinkDestSecure, err := cdctest.StartMockWebhookSinkSecure(cert)
		require.NoError(t, err)

		sinkDestSecureHost, err := url.Parse(sinkDestSecure.URL())
		require.NoError(t, err)

		clientCertPEM, clientKeyPEM, err := cdctest.GenerateClientCertAndKey(cert)
		require.NoError(t, err)

		params = sinkDestSecureHost.Query()
		params.Set(changefeedbase.SinkParamSkipTLSVerify, "true")
		params.Set(changefeedbase.SinkParamClientCert, base64.StdEncoding.EncodeToString(clientCertPEM))
		params.Set(changefeedbase.SinkParamClientKey, base64.StdEncoding.EncodeToString(clientKeyPEM))
		sinkDestSecureHost.RawQuery = params.Encode()

		details = jobspb.ChangefeedDetails{
			SinkURI: fmt.Sprintf("webhook-%s", sinkDestSecureHost.String()),
			Opts:    opts,
		}

		require.NoError(t, sinkSrc.Close())
		sinkSrc, err = setupWebhookSinkWithDetails(context.Background(), details, parallelism, timeutil.DefaultTimeSource{})
		require.NoError(t, err)

		// sink with client accepting server cert should pass
		testSendAndReceiveRows(t, sinkSrc, sinkDestSecure)

		require.NoError(t, sinkSrc.Close())
		require.NoError(t, sinkSrcNoCert.Close())
		require.NoError(t, sinkSrcInsecure.Close())
		require.NoError(t, sinkSrcWrongProtocol.Close())
		sinkDestHTTP.Close()
		sinkDestSecure.Close()
	}

	// run tests with parallelism from 1-4
	opts := getGenericWebhookSinkOptions()
	optsZeroValueConfig := getGenericWebhookSinkOptions(
		struct {
			key   string
			value string
		}{changefeedbase.OptWebhookSinkConfig,
			`{"Retry":{"Backoff": "5ms"},"Flush":{"Bytes": 0, "Frequency": "0s", "Messages": 0}}`})
	for i := 1; i <= 4; i++ {
		webhookSinkTestfn(i, opts.AsMap())
		webhookSinkTestfn(i, optsZeroValueConfig.AsMap())
	}
}

func TestWebhookSinkWithAuthOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	webhookSinkTestfn := func(parallelism int) {
		cert, _, err := cdctest.NewCACertBase64Encoded()
		require.NoError(t, err)

		username := "crl-user"
		password := "crl-pwd"
		var authHeader string
		cdctest.EncodeBase64ToString([]byte(fmt.Sprintf("%s:%s", username, password)), &authHeader)

		sinkDest, err := cdctest.StartMockWebhookSinkWithBasicAuth(cert, username, password)
		require.NoError(t, err)

		opts := getGenericWebhookSinkOptions(struct {
			key   string
			value string
		}{
			key:   changefeedbase.OptWebhookAuthHeader,
			value: fmt.Sprintf("Basic %s", authHeader),
		})

		sinkDestHost, err := url.Parse(sinkDest.URL())
		require.NoError(t, err)

		params := sinkDestHost.Query()
		params.Set(changefeedbase.SinkParamSkipTLSVerify, "true")
		sinkDestHost.RawQuery = params.Encode()

		details := jobspb.ChangefeedDetails{
			SinkURI: fmt.Sprintf("webhook-%s", sinkDestHost.String()),
			Opts:    opts.AsMap(),
		}

		sinkSrc, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism, timeutil.DefaultTimeSource{})
		require.NoError(t, err)

		testSendAndReceiveRows(t, sinkSrc, sinkDest)

		// no credentials should result in a 401
		delete(details.Opts, changefeedbase.OptWebhookAuthHeader)
		sinkSrcNoCreds, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism, timeutil.DefaultTimeSource{})
		require.NoError(t, err)
		require.NoError(t, sinkSrcNoCreds.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, zeroAlloc))

		require.EqualError(t, sinkSrcNoCreds.Flush(context.Background()), "401 Unauthorized: ")

		// wrong credentials should result in a 401 as well
		var wrongAuthHeader string
		cdctest.EncodeBase64ToString([]byte(fmt.Sprintf("%s:%s", username, "wrong-password")), &wrongAuthHeader)
		details.Opts[changefeedbase.OptWebhookAuthHeader] = fmt.Sprintf("Basic %s", wrongAuthHeader)
		sinkSrcWrongCreds, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism, timeutil.DefaultTimeSource{})
		require.NoError(t, err)

		require.NoError(t, sinkSrcWrongCreds.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, zeroAlloc))

		require.EqualError(t, sinkSrcWrongCreds.Flush(context.Background()), "401 Unauthorized: ")

		require.NoError(t, sinkSrc.Close())
		require.NoError(t, sinkSrcNoCreds.Close())
		require.NoError(t, sinkSrcWrongCreds.Close())
		sinkDest.Close()
	}

	// run tests with parallelism from 1-4
	for i := 1; i <= 4; i++ {
		webhookSinkTestfn(i)
	}
}

func TestWebhookSinkConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var pool testAllocPool
	retryThenSuccessFn := func(parallelism int) {
		opts := getGenericWebhookSinkOptions(struct {
			key   string
			value string
		}{
			key:   changefeedbase.OptWebhookSinkConfig,
			value: `{"Retry":{"Backoff": "5ms", "Max": 6}}`,
		})
		cert, certEncoded, err := cdctest.NewCACertBase64Encoded()
		require.NoError(t, err)
		sinkDest, err := cdctest.StartMockWebhookSink(cert)
		require.NoError(t, err)

		// alternates returning 2 errors (2 retries), then OK
		sinkDest.SetStatusCodes([]int{http.StatusInternalServerError,
			http.StatusBadGateway, http.StatusOK})

		sinkDestHost, err := url.Parse(sinkDest.URL())
		require.NoError(t, err)

		params := sinkDestHost.Query()
		params.Set(changefeedbase.SinkParamCACert, certEncoded)
		sinkDestHost.RawQuery = params.Encode()

		details := jobspb.ChangefeedDetails{
			SinkURI: fmt.Sprintf("webhook-%s", sinkDestHost.String()),
			Opts:    opts.AsMap(),
		}

		sinkSrc, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism, timeutil.DefaultTimeSource{})
		require.NoError(t, err)

		testSendAndReceiveRows(t, sinkSrc, sinkDest)
		// ensure that failures are retried twice (+1 for initial) before success
		// 4 messages sent, 4 * 3 = 12 in total
		require.Equal(t, 12, sinkDest.GetNumCalls())

		require.NoError(t, sinkSrc.Close())

		sinkDest.Close()
	}

	retryThenFailureDefaultFn := func(parallelism int) {
		opts := getGenericWebhookSinkOptions()
		cert, certEncoded, err := cdctest.NewCACertBase64Encoded()
		require.NoError(t, err)
		sinkDest, err := cdctest.StartMockWebhookSink(cert)
		require.NoError(t, err)

		// error out indefinitely
		sinkDest.SetStatusCodes([]int{http.StatusInternalServerError})

		sinkDestHost, err := url.Parse(sinkDest.URL())
		require.NoError(t, err)

		params := sinkDestHost.Query()
		params.Set(changefeedbase.SinkParamCACert, certEncoded)
		sinkDestHost.RawQuery = params.Encode()

		details := jobspb.ChangefeedDetails{
			SinkURI: fmt.Sprintf("webhook-%s", sinkDestHost.String()),
			Opts:    opts.AsMap(),
		}

		sinkSrc, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism, timeutil.DefaultTimeSource{})
		require.NoError(t, err)

		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, zeroAlloc))

		require.EqualError(t, sinkSrc.Flush(context.Background()), "500 Internal Server Error: ")

		// ensure that failures are retried the expected maximum number of times
		// before returning error
		require.Equal(t, defaultRetryConfig().MaxRetries+1, sinkDest.GetNumCalls())

		require.NoError(t, sinkSrc.Close())
		sinkDest.Close()
	}

	retryThenFailureCustomFn := func(parallelism int) {
		opts := getGenericWebhookSinkOptions(struct {
			key   string
			value string
		}{
			key:   changefeedbase.OptWebhookSinkConfig,
			value: `{"Retry":{"Backoff": "5ms", "Max": "6"}}`})
		cert, certEncoded, err := cdctest.NewCACertBase64Encoded()
		require.NoError(t, err)
		sinkDest, err := cdctest.StartMockWebhookSink(cert)
		require.NoError(t, err)

		// error out indefinitely
		sinkDest.SetStatusCodes([]int{http.StatusInternalServerError})

		sinkDestHost, err := url.Parse(sinkDest.URL())
		require.NoError(t, err)

		params := sinkDestHost.Query()
		params.Set(changefeedbase.SinkParamCACert, certEncoded)
		sinkDestHost.RawQuery = params.Encode()

		details := jobspb.ChangefeedDetails{
			SinkURI: fmt.Sprintf("webhook-%s", sinkDestHost.String()),
			Opts:    opts.AsMap(),
		}

		sinkSrc, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism, timeutil.DefaultTimeSource{})
		require.NoError(t, err)

		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, pool.alloc()))

		require.EqualError(t, sinkSrc.Flush(context.Background()), "500 Internal Server Error: ")

		// ensure that failures are retried the expected maximum number of times
		// before returning error
		require.Equal(t, 7, sinkDest.GetNumCalls())

		require.NoError(t, sinkSrc.Close())
		sinkDest.Close()
	}

	largeBatchSizeFn := func(parallelism int) {
		opts := getGenericWebhookSinkOptions(struct {
			key   string
			value string
		}{
			key:   changefeedbase.OptWebhookSinkConfig,
			value: `{"Retry":{"Backoff": "5ms"},"Flush":{"Messages": 5, "Frequency": "1h"}}`,
		})
		cert, certEncoded, err := cdctest.NewCACertBase64Encoded()
		require.NoError(t, err)
		sinkDest, err := cdctest.StartMockWebhookSink(cert)
		require.NoError(t, err)

		sinkDestHost, err := url.Parse(sinkDest.URL())
		require.NoError(t, err)

		params := sinkDestHost.Query()
		params.Set(changefeedbase.SinkParamCACert, certEncoded)
		sinkDestHost.RawQuery = params.Encode()

		details := jobspb.ChangefeedDetails{
			SinkURI: fmt.Sprintf("webhook-%s", sinkDestHost.String()),
			Opts:    opts.AsMap(),
		}

		mt := timeutil.NewManualTime(timeutil.Now())

		sinkSrc, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism, mt)
		require.NoError(t, err)

		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, pool.alloc()))
		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1001},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, pool.alloc()))
		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1002},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, pool.alloc()))
		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1003},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, pool.alloc()))
		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1004},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, pool.alloc()))

		require.NoError(t, sinkSrc.Flush(context.Background()))
		require.Equal(t, sinkDest.Pop(), `{"payload":[{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"},`+
			`{"after":{"col1":"val1","rowid":1001},"key":[1001],"topic:":"foo"},`+
			`{"after":{"col1":"val1","rowid":1002},"key":[1001],"topic:":"foo"},`+
			`{"after":{"col1":"val1","rowid":1003},"key":[1001],"topic:":"foo"},`+
			`{"after":{"col1":"val1","rowid":1004},"key":[1001],"topic:":"foo"}],"length":5}`)

		testutils.SucceedsSoon(t, func() error {
			// wait for the timer in batch worker to be set (1 hour from now, as specified by config) before advancing time.
			if len(mt.Timers()) == 1 && mt.Timers()[0] == mt.Now().Add(time.Hour) {
				return nil
			}
			return errors.New("Waiting for timer to be created by batch worker")
		})

		// check that the leftover timer from the previous batch doesn't trigger
		// an empty batch to be sent
		mt.Advance(time.Hour)
		require.Equal(t, sinkDest.Latest(), "")

		// messages without a full batch should not send
		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, pool.alloc()))
		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1001},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, pool.alloc()))
		require.Equal(t, sinkDest.Latest(), "")

		require.NoError(t, sinkSrc.Close())
		sinkDest.Close()
	}

	largeBatchBytesFn := func(parallelism int) {
		opts := getGenericWebhookSinkOptions(struct {
			key   string
			value string
		}{
			key:   changefeedbase.OptWebhookSinkConfig,
			value: `{"Retry":{"Backoff": "5ms"},"Flush":{"Bytes": 330, "Frequency": "1h"}}`,
		})
		cert, certEncoded, err := cdctest.NewCACertBase64Encoded()
		require.NoError(t, err)
		sinkDest, err := cdctest.StartMockWebhookSink(cert)
		require.NoError(t, err)

		sinkDestHost, err := url.Parse(sinkDest.URL())
		require.NoError(t, err)

		params := sinkDestHost.Query()
		params.Set(changefeedbase.SinkParamCACert, certEncoded)
		sinkDestHost.RawQuery = params.Encode()

		details := jobspb.ChangefeedDetails{
			SinkURI: fmt.Sprintf("webhook-%s", sinkDestHost.String()),
			Opts:    opts.AsMap(),
		}

		sinkSrc, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism, timeutil.DefaultTimeSource{})
		require.NoError(t, err)

		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, pool.alloc()))
		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1001},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, pool.alloc()))
		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1002},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, pool.alloc()))
		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1003},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, pool.alloc()))
		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1004},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, pool.alloc()))

		require.NoError(t, sinkSrc.Flush(context.Background()))
		require.Equal(t, sinkDest.Pop(), `{"payload":[{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"},`+
			`{"after":{"col1":"val1","rowid":1001},"key":[1001],"topic:":"foo"},`+
			`{"after":{"col1":"val1","rowid":1002},"key":[1001],"topic:":"foo"},`+
			`{"after":{"col1":"val1","rowid":1003},"key":[1001],"topic:":"foo"},`+
			`{"after":{"col1":"val1","rowid":1004},"key":[1001],"topic:":"foo"}],"length":5}`)

		// messages without a full batch should not send
		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, pool.alloc()))
		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1001},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, pool.alloc()))
		require.Equal(t, sinkDest.Latest(), "")

		require.NoError(t, sinkSrc.Close())
		sinkDest.Close()
	}

	largeBatchFrequencyFn := func(parallelism int) {
		opts := getGenericWebhookSinkOptions(struct {
			key   string
			value string
		}{
			key:   changefeedbase.OptWebhookSinkConfig,
			value: `{"Retry":{"Backoff": "5ms"},"Flush":{"Messages": 10, "Frequency": "1h"}}`,
		})
		cert, certEncoded, err := cdctest.NewCACertBase64Encoded()
		require.NoError(t, err)
		sinkDest, err := cdctest.StartMockWebhookSink(cert)
		require.NoError(t, err)

		sinkDestHost, err := url.Parse(sinkDest.URL())
		require.NoError(t, err)

		params := sinkDestHost.Query()
		params.Set(changefeedbase.SinkParamCACert, certEncoded)
		sinkDestHost.RawQuery = params.Encode()

		details := jobspb.ChangefeedDetails{
			SinkURI: fmt.Sprintf("webhook-%s", sinkDestHost.String()),
			Opts:    opts.AsMap(),
		}

		mt := timeutil.NewManualTime(timeutil.Now())

		sinkSrc, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism, mt)
		require.NoError(t, err)

		batchingSink, ok := sinkSrc.(*batchingSink)
		require.True(t, ok)
		var appendCount int32 = 0
		batchingSink.knobs.OnAppend = func(event *rowEvent) {
			atomic.AddInt32(&appendCount, 1)
		}

		// send incomplete batch
		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, pool.alloc()))
		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1001},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, pool.alloc()))

		// no messages at first
		require.Equal(t, sinkDest.Latest(), "")

		testutils.SucceedsSoon(t, func() error {
			if atomic.LoadInt32(&appendCount) >= 2 {
				return nil
			}
			return errors.New("Waiting for rows to be buffered")
		})
		mt.Advance(time.Hour)
		require.NoError(t, sinkSrc.Flush(context.Background()))
		// batch should send after time expires even if message quota has not been met
		require.Equal(t, sinkDest.Pop(), `{"payload":[{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"},`+
			`{"after":{"col1":"val1","rowid":1001},"key":[1001],"topic:":"foo"}],"length":2}`)

		mt.Advance(time.Hour)
		require.NoError(t, sinkSrc.Flush(context.Background()))
		// batch doesn't send if there are no messages to send
		require.Equal(t, sinkDest.Latest(), "")

		require.NoError(t, sinkSrc.Close())
		sinkDest.Close()
	}

	// run tests with parallelism from 1-4
	for i := 1; i <= 4; i++ {
		retryThenSuccessFn(i)
		retryThenFailureDefaultFn(i)
		retryThenFailureCustomFn(i)
		largeBatchSizeFn(i)
		largeBatchBytesFn(i)
		largeBatchFrequencyFn(i)
	}
}

func TestWebhookSinkShutsDownOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	webhookSinkTestfn := func(parallelism int) {
		ctx := context.Background()
		cert, certEncoded, err := cdctest.NewCACertBase64Encoded()
		require.NoError(t, err)
		sinkDest, err := cdctest.StartMockWebhookSink(cert)
		require.NoError(t, err)

		opts := getGenericWebhookSinkOptions()

		// retry and fail, then return OK
		sinkDest.SetStatusCodes(repeatStatusCode(http.StatusInternalServerError,
			defaultRetryConfig().MaxRetries+1))
		sinkDestHost, err := url.Parse(sinkDest.URL())
		require.NoError(t, err)

		params := sinkDestHost.Query()
		params.Set(changefeedbase.SinkParamCACert, certEncoded)
		sinkDestHost.RawQuery = params.Encode()

		details := jobspb.ChangefeedDetails{
			SinkURI: fmt.Sprintf("webhook-%s", sinkDestHost.String()),
			Opts:    opts.AsMap(),
		}

		sinkSrc, err := setupWebhookSinkWithDetails(ctx, details, parallelism, timeutil.DefaultTimeSource{})
		require.NoError(t, err)

		require.NoError(t, sinkSrc.EmitRow(ctx, noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, zeroAlloc))
		// error should be propagated immediately in the next call
		require.EqualError(t, sinkSrc.Flush(ctx), "500 Internal Server Error: ")

		// check that no messages have been delivered
		require.Equal(t, "", sinkDest.Pop())

		sinkDest.Close()
		require.NoError(t, sinkSrc.Close())
	}

	// run tests with parallelism from 1-4
	for i := 1; i <= 4; i++ {
		webhookSinkTestfn(i)
	}
}

// Regression test for https://github.com/cockroachdb/cockroach/issues/102467.
// Ensure that we do not use the default retry config which is capped at
// 4000ms.
func TestWebhookSinkRetryDuration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderShort(t)

	opts := getGenericWebhookSinkOptions(struct {
		key   string
		value string
	}{
		key:   changefeedbase.OptWebhookSinkConfig,
		value: `{"Retry":{"Backoff": "2s", "Max": "2"}}`})
	webhookOpts, err := opts.GetWebhookSinkOptions()
	require.NoError(t, err)

	_, retryCfg, err := getSinkConfigFromJson(webhookOpts.JSONConfig,
		sinkJSONConfig{})
	require.NoError(t, err)
	require.Equal(t, retryCfg.MaxBackoff, 30*time.Second)
}

// Regression test for #118485.
func TestWebhookSinkRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderShort(t)

	ctx := context.Background()
	opts := getGenericWebhookSinkOptions(struct {
		key   string
		value string
	}{
		key: changefeedbase.OptWebhookSinkConfig,
		// Note: The original issue repros most successfully with a 60s backoff,
		// which is higher than we support and is higher than desirable for testing.
		value: `{"Retry":{"Backoff": "500ms", "Max": "2"}}`})
	cert, certEncoded, err := cdctest.NewCACertBase64Encoded()
	require.NoError(t, err)

	sinkDest, err := cdctest.StartMockWebhookSink(cert)
	require.NoError(t, err)
	// Return an error that retries in the sink client, then ok.
	sinkDest.SetStatusCodes([]int{http.StatusTooManyRequests, http.StatusOK})

	sinkDestHost, err := url.Parse(sinkDest.URL())
	require.NoError(t, err)

	params := sinkDestHost.Query()
	params.Set(changefeedbase.SinkParamCACert, certEncoded)
	sinkDestHost.RawQuery = params.Encode()

	details := jobspb.ChangefeedDetails{
		SinkURI: fmt.Sprintf("webhook-%s", sinkDestHost.String()),
		Opts:    opts.AsMap(),
	}

	sinkSrc, err := setupWebhookSinkWithDetails(ctx, details, 1 /* parallelism */, timeutil.DefaultTimeSource{})
	require.NoError(t, err)

	require.NoError(t, sinkSrc.EmitRow(ctx, noTopic{}, []byte("[1001]"), []byte(`{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"}`), zeroTS, zeroTS, zeroAlloc))
	require.NoError(t, sinkSrc.Flush(ctx))

	require.Equal(t, `{"payload":[{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"}],"length":1}`, sinkDest.Pop())

	sinkDest.Close()
	require.NoError(t, sinkSrc.Close())
}

func TestInvalidWebhookSinkCompression(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cert, certEncoded, err := cdctest.NewCACertBase64Encoded()
	require.NoError(t, err)

	sinkDest, err := cdctest.StartMockWebhookSink(cert)
	require.NoError(t, err)
	defer sinkDest.Close()

	sinkDestHost, err := url.Parse(sinkDest.URL())
	require.NoError(t, err)

	params := sinkDestHost.Query()
	params.Set(changefeedbase.SinkParamCACert, certEncoded)
	sinkDestHost.RawQuery = params.Encode()

	invalidOpts := getGenericWebhookSinkOptions(struct {
		key   string
		value string
	}{
		key:   changefeedbase.OptCompression,
		value: "invalid",
	})

	details := jobspb.ChangefeedDetails{
		SinkURI: fmt.Sprintf("webhook-%s", sinkDestHost.String()),
		Opts:    invalidOpts.AsMap(),
	}

	_, err = setupWebhookSinkWithDetails(context.Background(), details, 1, timeutil.DefaultTimeSource{})
	require.Error(t, err)
	require.Contains(t, err.Error(), `unsupported compression type "invalid"`)
}

func TestWebhookSinkCompression(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name        string
		compression string
	}{
		{
			name:        "gzip compression",
			compression: "gzip",
		},
		{
			name:        "zstd compression",
			compression: "zstd",
		},
	}

	webhookSinkCompressionTestFn := func(compression string, parallelism int) {
		// Create a test certificate
		cert, certEncoded, err := cdctest.NewCACertBase64Encoded()
		require.NoError(t, err)

		// Start mock webhook sink
		sinkDest, err := cdctest.StartMockWebhookSink(cert)
		require.NoError(t, err)

		// Get sink options with compression enabled
		opts := getGenericWebhookSinkOptions(struct {
			key   string
			value string
		}{
			key:   changefeedbase.OptCompression,
			value: compression,
		})

		sinkDestHost, err := url.Parse(sinkDest.URL())
		require.NoError(t, err)

		params := sinkDestHost.Query()
		params.Set(changefeedbase.SinkParamCACert, certEncoded)
		sinkDestHost.RawQuery = params.Encode()

		details := jobspb.ChangefeedDetails{
			SinkURI: fmt.Sprintf("webhook-%s", sinkDestHost.String()),
			Opts:    opts.AsMap(),
		}

		sinkSrc, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism, timeutil.DefaultTimeSource{})
		require.NoError(t, err)

		// Test with compression
		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{},
			[]byte("[1001]"),
			[]byte(`{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"}`),
			zeroTS,
			zeroTS,
			zeroAlloc))
		require.NoError(t, sinkSrc.Flush(context.Background()))

		// Verify compression headers are present
		require.Equal(t, compression, sinkDest.LastRequestHeaders().Get("Content-Encoding"))
		require.Equal(t, compression, sinkDest.LastRequestHeaders().Get("Accept-Encoding"))

		// Verify the content can be decompressed and matches expected
		expected := `{"payload":[{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"}],"length":1}`
		require.Equal(t, expected, sinkDest.Latest())

		require.NoError(t, sinkSrc.Close())
		sinkDest.Close()
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for i := 1; i <= 4; i++ {
				parallelism := i
				t.Run(fmt.Sprintf("parallelism-%d", parallelism), func(t *testing.T) {
					webhookSinkCompressionTestFn(tc.compression, parallelism)
				})
			}
		})
	}
}

func TestWebhookSinkCompressionWithBatching(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name        string
		compression string
	}{
		{
			name:        "gzip compression",
			compression: "gzip",
		},
		{
			name:        "zstd compression",
			compression: "zstd",
		},
	}

	batchingWithCompressionTestFn := func(compression string, parallelism int) {
		cert, certEncoded, err := cdctest.NewCACertBase64Encoded()
		require.NoError(t, err)
		sinkDest, err := cdctest.StartMockWebhookSink(cert)
		require.NoError(t, err)

		// Configure both compression and batching
		opts := getGenericWebhookSinkOptions(
			[]struct {
				key   string
				value string
			}{{
				key:   changefeedbase.OptCompression,
				value: compression,
			}, {
				key:   changefeedbase.OptWebhookSinkConfig,
				value: `{"Flush":{"Messages": 2, "Frequency": "1h"}}`,
			}}...,
		)

		sinkDestHost, err := url.Parse(sinkDest.URL())
		require.NoError(t, err)

		params := sinkDestHost.Query()
		params.Set(changefeedbase.SinkParamCACert, certEncoded)
		sinkDestHost.RawQuery = params.Encode()

		details := jobspb.ChangefeedDetails{
			SinkURI: fmt.Sprintf("webhook-%s", sinkDestHost.String()),
			Opts:    opts.AsMap(),
		}

		mt := timeutil.NewManualTime(timeutil.Now())
		sinkSrc, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism, mt)
		require.NoError(t, err)

		// Send first message - should not trigger batch
		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{},
			[]byte("[1001]"),
			[]byte(`{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"}`),
			zeroTS,
			zeroTS,
			zeroAlloc))
		require.Equal(t, "", sinkDest.Latest())

		// Send second message - should trigger batch
		require.NoError(t, sinkSrc.EmitRow(context.Background(), noTopic{},
			[]byte("[1002]"),
			[]byte(`{"after":{"col1":"val2","rowid":1001},"key":[1002],"topic:":"foo"}`),
			zeroTS,
			zeroTS,
			zeroAlloc))
		require.NoError(t, sinkSrc.Flush(context.Background()))

		// Verify compression headers
		require.Equal(t, compression, sinkDest.LastRequestHeaders().Get("Content-Encoding"))
		require.Equal(t, compression, sinkDest.LastRequestHeaders().Get("Accept-Encoding"))

		// Verify batched content
		expected := `{"payload":[` +
			`{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"},` +
			`{"after":{"col1":"val2","rowid":1001},"key":[1002],"topic:":"foo"}` +
			`],"length":2}`
		require.Equal(t, expected, sinkDest.Latest())

		require.NoError(t, sinkSrc.Close())
		sinkDest.Close()
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for i := 1; i <= 4; i++ {
				parallelism := i
				t.Run(fmt.Sprintf("parallelism-%d", parallelism), func(t *testing.T) {
					batchingWithCompressionTestFn(tc.compression, parallelism)
				})
			}
		})
	}
}

func TestWebhookSinkErrorCompressedResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name        string
		compression string
	}{
		{
			name:        "gzip compression",
			compression: "gzip",
		},
		{
			name:        "zstd compression",
			compression: "zstd",
		},
	}

	webhookSinkCompressedErrorTestFn := func(compression string, parallelism int) {
		ctx := context.Background()
		cert, certEncoded, err := cdctest.NewCACertBase64Encoded()
		require.NoError(t, err)
		sinkDest, err := cdctest.StartMockWebhookSink(cert)
		require.NoError(t, err)

		// Configure sink to use specified compression
		opts := getGenericWebhookSinkOptions(struct {
			key   string
			value string
		}{
			key:   changefeedbase.OptCompression,
			value: compression,
		})

		// Configure error response
		responseBody := "Test error response"
		sinkDest.ClearResponses()
		sinkDest.SetResponse(http.StatusInternalServerError, []byte(responseBody))

		sinkDestHost, err := url.Parse(sinkDest.URL())
		require.NoError(t, err)

		params := sinkDestHost.Query()
		params.Set(changefeedbase.SinkParamCACert, certEncoded)
		sinkDestHost.RawQuery = params.Encode()

		details := jobspb.ChangefeedDetails{
			SinkURI: fmt.Sprintf("webhook-%s", sinkDestHost.String()),
			Opts:    opts.AsMap(),
		}

		sinkSrc, err := setupWebhookSinkWithDetails(ctx, details, parallelism, timeutil.DefaultTimeSource{})
		require.NoError(t, err)

		// Send data and expect error with compressed response
		require.NoError(t, sinkSrc.EmitRow(ctx, noTopic{},
			[]byte("[1001]"),
			[]byte(`{"after":{"col1":"val1","rowid":1000},"key":[1001],"topic:":"foo"}`),
			zeroTS,
			zeroTS,
			zeroAlloc))

		err = sinkSrc.Flush(ctx)
		require.Error(t, err)
		// Verify error body is decompressed
		require.Equal(t, fmt.Sprintf(`500 Internal Server Error: %s`, responseBody), err.Error())

		// Verify no messages delivered
		require.Equal(t, "", sinkDest.Pop())

		require.NoError(t, sinkSrc.Close())
		sinkDest.Close()
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for i := 1; i <= 4; i++ {
				parallelism := i
				t.Run(fmt.Sprintf("parallelism-%d", parallelism), func(t *testing.T) {
					webhookSinkCompressedErrorTestFn(tc.compression, parallelism)
				})
			}
		})
	}
}
