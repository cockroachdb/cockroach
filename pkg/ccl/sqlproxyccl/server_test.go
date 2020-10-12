// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/stretchr/testify/require"
)

func TestHandleHealth(t *testing.T) {
	proxyServer := NewServer()

	testServer := httptest.NewServer(proxyServer.mux)
	defer testServer.Close()

	resp, err := http.Get(fmt.Sprintf("%s/_status/healthz/", testServer.URL))
	require.NoError(t, err)

	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	out, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, []byte("OK"), out)
}

func TestHandleVars(t *testing.T) {
	proxyServer := NewServer()

	testServer := httptest.NewServer(proxyServer.mux)
	defer testServer.Close()

	resp, err := http.Get(fmt.Sprintf("%s/_status/vars/", testServer.URL))
	require.NoError(t, err)

	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(
		t,
		httputil.PlaintextContentType,
		resp.Header.Get(httputil.ContentTypeHeader),
	)

	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Contains(t, string(body), "# HELP proxy_sql_conns")
}
