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

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
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

func setupWebhookSinkWithDetails(details jobspb.ChangefeedDetails) (Sink, error) {
	serverCfg := &execinfra.ServerConfig{Settings: cluster.MakeTestingClusterSettings()}
	sinkSrc, err := getSink(context.Background(), serverCfg, details, nil,
		security.SQLUsername{}, mon.BoundAccount{}, 0)
	if err != nil {
		return nil, err
	}

	return sinkSrc, nil
}

// general happy path for webhook sink
func testSendAndReceiveRows(t *testing.T, sinkSrc Sink, sinkDest *cdctest.MockWebhookSink) {
	ctx := context.Background()

	// test an insert row entry
	err := sinkSrc.EmitRow(ctx, nil, []byte("[1001]"),
		[]byte("{\"after\":{\"col1\":\"val1\",\"rowid\":1000},\"key\":[1001],\"topic:\":\"foo\"}"), hlc.Timestamp{})
	if err != nil {
		t.Fatal(err)
	}
	err = sinkSrc.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t,
		"{\"payload\":[{\"after\":{\"col1\":\"val1\",\"rowid\":1000},\"key\":[1001],\"topic:\":\"foo\"}]}", sinkDest.Latest(),
		"sink %s expected to receive message %s", sinkDest.URL(),
		"{\"payload\":[{\"after\":{\"col1\":\"val1\",\"rowid\":1000},\"key\":[1001],\"topic:\":\"foo\"}]}")

	// test a delete row entry
	err = sinkSrc.EmitRow(ctx, nil, []byte("[1002]"), []byte("{\"after\":null,\"key\":[1002],\"topic:\":\"foo\"}"), hlc.Timestamp{})
	if err != nil {
		t.Fatal(err)
	}
	err = sinkSrc.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t,
		"{\"payload\":[{\"after\":null,\"key\":[1002],\"topic:\":\"foo\"}]}", sinkDest.Latest(),
		"sink %s expected to receive message %s", sinkDest.URL(),
		"{\"payload\":[{\"after\":null,\"key\":[1002],\"topic:\":\"foo\"}]}")
}

func TestWebhookSink(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cert, certEncoded, err := cdctest.NewCACertBase64Encoded()
	if err != nil {
		t.Fatal(err)
	}
	sinkDest, err := cdctest.StartMockWebhookSink(cert)
	if err != nil {
		t.Fatal(err)
	}

	opts := getGenericWebhookSinkOptions()

	sinkDestHost, err := url.Parse(sinkDest.URL())
	if err != nil {
		t.Fatal(err)
	}

	params := sinkDestHost.Query()
	params.Set(changefeedbase.SinkParamCACert, certEncoded)
	sinkDestHost.RawQuery = params.Encode()

	details := jobspb.ChangefeedDetails{
		SinkURI: fmt.Sprintf("webhook-%s", sinkDestHost.String()),
		Opts:    opts,
	}

	sinkSrc, err := setupWebhookSinkWithDetails(details)
	if err != nil {
		t.Fatal(err)
	}

	// sink with client accepting server cert should pass
	testSendAndReceiveRows(t, sinkSrc, sinkDest)

	params.Del(changefeedbase.SinkParamCACert)
	sinkDestHost.RawQuery = params.Encode()
	details.SinkURI = fmt.Sprintf("webhook-%s", sinkDestHost.String())
	sinkSrcNoCert, err := setupWebhookSinkWithDetails(details)
	if err != nil {
		t.Fatal(err)
	}

	// now sink's client accepts no custom certs, should reject the server's cert and fail
	err = sinkSrcNoCert.EmitRow(context.Background(), nil, []byte("[1001]"),
		[]byte("{\"after\":{\"col1\":\"val1\",\"rowid\":1000},\"key\":[1001],\"topic:\":\"foo\"}"), hlc.Timestamp{})
	require.EqualError(t, err, fmt.Sprintf(`Post "%s": x509: certificate signed by unknown authority`, sinkDest.URL()))

	err = sinkSrcNoCert.Flush(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	params.Set(changefeedbase.SinkParamSkipTLSVerify, "true")
	sinkDestHost.RawQuery = params.Encode()
	details.SinkURI = fmt.Sprintf("webhook-%s", sinkDestHost.String())
	sinkSrcInsecure, err := setupWebhookSinkWithDetails(details)
	if err != nil {
		t.Fatal(err)
	}

	// client should allow unrecognized certs and pass
	testSendAndReceiveRows(t, sinkSrcInsecure, sinkDest)

	// sink should throw an error if a non-2XX status code is returned
	sinkDest.SetStatusCode(http.StatusBadGateway)
	err = sinkSrc.EmitRow(context.Background(), nil, []byte("[1001]"),
		[]byte("{\"after\":{\"col1\":\"val1\",\"rowid\":1000},\"key\":[1001],\"topic:\":\"foo\"}"), hlc.Timestamp{})
	require.EqualError(t, err, "502 Bad Gateway: ")

	err = sinkSrc.Flush(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// sink should throw an error if server is unreachable
	sinkDest.Close()
	err = sinkSrc.EmitRow(context.Background(), nil, []byte("[1001]"),
		[]byte("{\"after\":{\"col1\":\"val1\",\"rowid\":1000},\"key\":[1001],\"topic:\":\"foo\"}"), hlc.Timestamp{})
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf(`Post "%s":`, sinkDest.URL()))

	err = sinkSrc.Flush(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	err = sinkSrc.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = sinkSrcNoCert.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = sinkSrcInsecure.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestWebhookSinkWithAuthOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cert, _, err := cdctest.NewCACertBase64Encoded()
	if err != nil {
		t.Fatal(err)
	}

	username := "crl-user"
	password := "crl-pwd"
	var authHeader string
	cdctest.EncodeBase64ToString([]byte(fmt.Sprintf("%s:%s", username, password)), &authHeader)

	sinkDest, err := cdctest.StartMockWebhookSinkWithBasicAuth(cert, username, password)
	if err != nil {
		t.Fatal(err)
	}

	opts := getGenericWebhookSinkOptions()
	opts[changefeedbase.OptWebhookAuthHeader] = fmt.Sprintf("Basic %s", authHeader)

	sinkDestHost, err := url.Parse(sinkDest.URL())
	if err != nil {
		t.Fatal(err)
	}

	params := sinkDestHost.Query()
	params.Set(changefeedbase.SinkParamSkipTLSVerify, "true")
	sinkDestHost.RawQuery = params.Encode()

	details := jobspb.ChangefeedDetails{
		SinkURI: fmt.Sprintf("webhook-%s", sinkDestHost.String()),
		Opts:    opts,
	}

	sinkSrc, err := setupWebhookSinkWithDetails(details)
	if err != nil {
		t.Fatal(err)
	}

	testSendAndReceiveRows(t, sinkSrc, sinkDest)

	// no credentials should result in a 401
	delete(opts, changefeedbase.OptWebhookAuthHeader)
	sinkSrcNoCreds, err := setupWebhookSinkWithDetails(details)
	if err != nil {
		t.Fatal(err)
	}
	err = sinkSrcNoCreds.EmitRow(context.Background(), nil, []byte("[1001]"),
		[]byte("{\"after\":{\"col1\":\"val1\",\"rowid\":1000},\"key\":[1001],\"topic:\":\"foo\"}"), hlc.Timestamp{})
	require.EqualError(t, err, "401 Unauthorized: ")

	err = sinkSrcNoCreds.Flush(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// wrong credentials should result in a 401 as well
	var wrongAuthHeader string
	cdctest.EncodeBase64ToString([]byte(fmt.Sprintf("%s:%s", username, "wrong-password")), &wrongAuthHeader)
	opts[changefeedbase.OptWebhookAuthHeader] = fmt.Sprintf("Basic %s", wrongAuthHeader)
	sinkSrcWrongCreds, err := setupWebhookSinkWithDetails(details)
	if err != nil {
		t.Fatal(err)
	}

	err = sinkSrcWrongCreds.EmitRow(context.Background(), nil, []byte("[1001]"),
		[]byte("{\"after\":{\"col1\":\"val1\",\"rowid\":1000},\"key\":[1001],\"topic:\":\"foo\"}"), hlc.Timestamp{})
	require.EqualError(t, err, "401 Unauthorized: ")

	err = sinkSrcWrongCreds.Flush(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	err = sinkSrc.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = sinkSrcNoCreds.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = sinkSrcWrongCreds.Close()
	if err != nil {
		t.Fatal(err)
	}
	sinkDest.Close()
}
