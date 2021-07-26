// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/stretchr/testify/require"
)

func getGenericWebhookSinkOptions() map[string]string {
	opts := make(map[string]string)
	opts[changefeedbase.OptFormat] = string(changefeedbase.OptFormatJSON)
	opts[changefeedbase.OptKeyInValue] = ``
	opts[changefeedbase.OptEnvelope] = string(changefeedbase.OptEnvelopeWrapped)
	opts[changefeedbase.OptTopicInValue] = ``
	return opts
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
	ctx context.Context, details jobspb.ChangefeedDetails, parallelism int,
) (Sink, error) {
	u, err := url.Parse(details.SinkURI)
	if err != nil {
		return nil, err
	}

	// unlimited memory for testing purposes only
	memMon := mon.NewUnlimitedMonitor(context.Background(),
		"test mon",
		mon.MemoryResource,
		nil,  /* curCount */
		nil,  /* maxHist */
		1000, /* noteworthy */
		cluster.MakeTestingClusterSettings(),
	)

	defaultRetryCfg := defaultRetryConfig()
	// speed up test by using faster backoff times
	shortRetryCfg := retry.Options{
		MaxRetries:     defaultRetryCfg.MaxRetries,
		InitialBackoff: 5 * time.Millisecond,
	}

	sinkSrc, err := makeWebhookSink(ctx, sinkURL{URL: u}, details.Opts, parallelism, memMon.MakeBoundAccount(), shortRetryCfg)
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
	require.NoError(t, sinkSrc.EmitRow(ctx, nil, []byte("[1001]"),
		[]byte("{\"after\":{\"col1\":\"val1\",\"rowid\":1000},\"key\":[1001],\"topic:\":\"foo\"}"), hlc.Timestamp{}))
	require.NoError(t, sinkSrc.EmitRow(ctx, nil, []byte("[1001]"),
		[]byte("{\"after\":{\"col1\":\"val1\",\"rowid\":1002},\"key\":[1001],\"topic:\":\"foo\"}"), hlc.Timestamp{}))
	require.NoError(t, sinkSrc.Flush(ctx))

	require.Equal(t,
		"{\"payload\":[{\"after\":{\"col1\":\"val1\",\"rowid\":1002},\"key\":[1001],\"topic:\":\"foo\"}]}", sinkDest.Latest(),
		"sink %s expected to receive message %s", sinkDest.URL(),
		"{\"payload\":[{\"after\":{\"col1\":\"val1\",\"rowid\":1002},\"key\":[1001],\"topic:\":\"foo\"}]}")

	// test a delete row entry
	require.NoError(t, sinkSrc.EmitRow(ctx, nil, []byte("[1002]"), []byte("{\"after\":null,\"key\":[1002],\"topic:\":\"foo\"}"), hlc.Timestamp{}))
	require.NoError(t, sinkSrc.Flush(ctx))

	require.Equal(t,
		"{\"payload\":[{\"after\":null,\"key\":[1002],\"topic:\":\"foo\"}]}", sinkDest.Latest(),
		"sink %s expected to receive message %s", sinkDest.URL(),
		"{\"payload\":[{\"after\":null,\"key\":[1002],\"topic:\":\"foo\"}]}")

	enc, err := makeJSONEncoder(getGenericWebhookSinkOptions(), jobspb.ChangefeedTargets{})
	require.NoError(t, err)

	// test a resolved timestamp entry
	require.NoError(t, sinkSrc.EmitResolvedTimestamp(ctx, Encoder(enc), hlc.Timestamp{WallTime: 2}))
	require.NoError(t, sinkSrc.Flush(ctx))

	require.Equal(t,
		"{\"resolved\":\"2.0000000000\"}", sinkDest.Latest(),
		"sink %s expected to receive message %s", sinkDest.URL(),
		"{\"resolved\":\"2.0000000000\"}")
}

func TestWebhookSink(t *testing.T) {
	defer leaktest.AfterTest(t)()

	webhookSinkTestfn := func(parallelism int) {
		cert, certEncoded, err := cdctest.NewCACertBase64Encoded()
		require.NoError(t, err)
		sinkDest, err := cdctest.StartMockWebhookSink(cert)
		require.NoError(t, err)

		opts := getGenericWebhookSinkOptions()

		sinkDestHost, err := url.Parse(sinkDest.URL())
		require.NoError(t, err)

		params := sinkDestHost.Query()
		params.Set(changefeedbase.SinkParamCACert, certEncoded)
		sinkDestHost.RawQuery = params.Encode()

		details := jobspb.ChangefeedDetails{
			SinkURI: fmt.Sprintf("webhook-%s", sinkDestHost.String()),
			Opts:    opts,
		}

		sinkSrc, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism)
		require.NoError(t, err)

		// sink with client accepting server cert should pass
		testSendAndReceiveRows(t, sinkSrc, sinkDest)

		params.Del(changefeedbase.SinkParamCACert)
		sinkDestHost.RawQuery = params.Encode()
		details.SinkURI = fmt.Sprintf("webhook-%s", sinkDestHost.String())
		sinkSrcNoCert, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism)
		require.NoError(t, err)

		// now sink's client accepts no custom certs, should reject the server's cert and fail
		require.NoError(t, sinkSrcNoCert.EmitRow(context.Background(), nil, []byte("[1001]"),
			[]byte("{\"after\":{\"col1\":\"val1\",\"rowid\":1000},\"key\":[1001],\"topic:\":\"foo\"}"), hlc.Timestamp{}))

		require.EqualError(t, sinkSrcNoCert.Flush(context.Background()),
			fmt.Sprintf(`Post "%s": x509: certificate signed by unknown authority`, sinkDest.URL()))

		params.Set(changefeedbase.SinkParamSkipTLSVerify, "true")
		sinkDestHost.RawQuery = params.Encode()
		details.SinkURI = fmt.Sprintf("webhook-%s", sinkDestHost.String())
		sinkSrcInsecure, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism)
		require.NoError(t, err)

		// client should allow unrecognized certs and pass
		testSendAndReceiveRows(t, sinkSrcInsecure, sinkDest)

		// sink should throw an error if server is unreachable
		sinkDest.Close()
		require.NoError(t, sinkSrc.EmitRow(context.Background(), nil, []byte("[1001]"),
			[]byte("{\"after\":{\"col1\":\"val1\",\"rowid\":1000},\"key\":[1001],\"topic:\":\"foo\"}"), hlc.Timestamp{}))

		err = sinkSrc.Flush(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), fmt.Sprintf(`Post "%s":`, sinkDest.URL()))

		require.NoError(t, sinkSrc.Close())
		require.NoError(t, sinkSrcNoCert.Close())
		require.NoError(t, sinkSrcInsecure.Close())
	}

	// run tests with parallelism from 1-16 (1,2,4,8,16)
	for i := 1; i <= 16; i *= 2 {
		webhookSinkTestfn(i)
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

		opts := getGenericWebhookSinkOptions()
		opts[changefeedbase.OptWebhookAuthHeader] = fmt.Sprintf("Basic %s", authHeader)

		sinkDestHost, err := url.Parse(sinkDest.URL())
		require.NoError(t, err)

		params := sinkDestHost.Query()
		params.Set(changefeedbase.SinkParamSkipTLSVerify, "true")
		sinkDestHost.RawQuery = params.Encode()

		details := jobspb.ChangefeedDetails{
			SinkURI: fmt.Sprintf("webhook-%s", sinkDestHost.String()),
			Opts:    opts,
		}

		sinkSrc, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism)
		require.NoError(t, err)

		testSendAndReceiveRows(t, sinkSrc, sinkDest)

		// no credentials should result in a 401
		delete(opts, changefeedbase.OptWebhookAuthHeader)
		sinkSrcNoCreds, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism)
		require.NoError(t, err)
		require.NoError(t, sinkSrcNoCreds.EmitRow(context.Background(), nil, []byte("[1001]"),
			[]byte("{\"after\":{\"col1\":\"val1\",\"rowid\":1000},\"key\":[1001],\"topic:\":\"foo\"}"), hlc.Timestamp{}))

		require.EqualError(t, sinkSrcNoCreds.Flush(context.Background()), "401 Unauthorized: ")

		// wrong credentials should result in a 401 as well
		var wrongAuthHeader string
		cdctest.EncodeBase64ToString([]byte(fmt.Sprintf("%s:%s", username, "wrong-password")), &wrongAuthHeader)
		opts[changefeedbase.OptWebhookAuthHeader] = fmt.Sprintf("Basic %s", wrongAuthHeader)
		sinkSrcWrongCreds, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism)
		require.NoError(t, err)

		require.NoError(t, sinkSrcWrongCreds.EmitRow(context.Background(), nil, []byte("[1001]"),
			[]byte("{\"after\":{\"col1\":\"val1\",\"rowid\":1000},\"key\":[1001],\"topic:\":\"foo\"}"), hlc.Timestamp{}))

		require.EqualError(t, sinkSrcWrongCreds.Flush(context.Background()), "401 Unauthorized: ")

		require.NoError(t, sinkSrc.Close())
		require.NoError(t, sinkSrcNoCreds.Close())
		require.NoError(t, sinkSrcWrongCreds.Close())
		sinkDest.Close()
	}

	// run tests with parallelism from 1-16 (1,2,4,8,16)
	for i := 1; i <= 16; i *= 2 {
		webhookSinkTestfn(i)
	}
}

func TestWebhookSinkRetriesRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()

	opts := getGenericWebhookSinkOptions()

	retryThenSuccessFn := func(parallelism int) {
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
			Opts:    opts,
		}

		sinkSrc, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism)
		require.NoError(t, err)

		testSendAndReceiveRows(t, sinkSrc, sinkDest)
		// ensure that failures are retried twice (+1 for initial) before success
		// 4 messages sent, 4 * 3 = 12 in total
		require.Equal(t, 12, sinkDest.GetNumCalls())

		require.NoError(t, sinkSrc.Close())

		sinkDest.Close()
	}

	retryThenFailureFn := func(parallelism int) {
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
			Opts:    opts,
		}

		sinkSrc, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism)
		require.NoError(t, err)

		require.NoError(t, sinkSrc.EmitRow(context.Background(), nil, []byte("[1001]"),
			[]byte("{\"after\":{\"col1\":\"val1\",\"rowid\":1000},\"key\":[1001],\"topic:\":\"foo\"}"), hlc.Timestamp{}))

		require.EqualError(t, sinkSrc.Flush(context.Background()), "500 Internal Server Error: ")

		// ensure that failures are retried the default maximum number of times
		// before returning error
		require.Equal(t, defaultRetryConfig().MaxRetries+1, sinkDest.GetNumCalls())

		require.NoError(t, sinkSrc.Close())
		sinkDest.Close()
	}

	// run tests with parallelism from 1-16 (1,2,4,8,16)
	for i := 1; i <= 16; i *= 2 {
		retryThenSuccessFn(i)
		retryThenFailureFn(i)
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
			Opts:    opts,
		}

		sinkSrc, err := setupWebhookSinkWithDetails(ctx, details, parallelism)
		require.NoError(t, err)

		require.NoError(t, sinkSrc.EmitRow(ctx, nil, []byte("[1001]"),
			[]byte("{\"after\":{\"col1\":\"val1\",\"rowid\":1000},\"key\":[1001],\"topic:\":\"foo\"}"), hlc.Timestamp{}))
		// error should be propagated immediately in the next call
		require.EqualError(t, sinkSrc.Flush(ctx), "500 Internal Server Error: ")

		// check that no messages have been delivered
		require.Equal(t, "", sinkDest.Pop())

		sinkDest.Close()
		require.NoError(t, sinkSrc.Close())
	}

	// run tests with parallelism from 1-16 (1,2,4,8,16)
	for i := 1; i <= 16; i *= 2 {
		webhookSinkTestfn(i)
	}
}
