// Copyright 2024 The Cockroach Authors.

// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package registryhandler

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
)

func TestRegisterHandlers(t *testing.T) {
	ctx := context.Background()
	invoked := ""
	require.Nil(t, RegisterHandlers(ctx, setupMockRegistry([]*URLRegistry{
		{
			Url:    "/test1",
			Method: GET,
			HandleFunc: func(w http.ResponseWriter, r *http.Request) {
				invoked = "test1"
			},
		},
		{
			Url:    "/test2",
			Method: POST,
			HandleFunc: func(w http.ResponseWriter, r *http.Request) {
				invoked = "test2"
			},
		},
		{
			Url:    "/test1",
			Method: POST,
			HandleFunc: func(w http.ResponseWriter, r *http.Request) {
				invoked = "test1-post"
			},
		},
		{
			Url:    "/test1/abc",
			Method: GET,
			HandleFunc: func(w http.ResponseWriter, r *http.Request) {
				invoked = "test1-abc"
			},
		},
		{
			Url:    "/test1/{param}",
			Method: GET,
			HandleFunc: func(w http.ResponseWriter, r *http.Request) {
				invoked = "test1-param"
			},
		},
		{
			Url:    "/test-skip",
			Method: GET,
			Skip:   true,
		},
	})))
	r, e := setup(t)
	defer teardown(ctx, t, r)
	require.Nil(t, e)
	require.Empty(t, invoked)
	res, e := httputil.Get(ctx, fmt.Sprintf("%s/test1", r.url))
	require.Nil(t, e)
	require.NotNil(t, res)
	require.Equal(t, "test1", invoked)
	res, e = httputil.Post(ctx, fmt.Sprintf("%s/test2", r.url), "", nil)
	require.Nil(t, e)
	require.NotNil(t, res)
	require.Equal(t, "test2", invoked)
	res, e = httputil.Post(ctx, fmt.Sprintf("%s/test1", r.url), "", nil)
	require.Nil(t, e)
	require.NotNil(t, res)
	require.Equal(t, "test1-post", invoked)
	res, e = httputil.Get(ctx, fmt.Sprintf("%s/test1/abc", r.url))
	require.Nil(t, e)
	require.NotNil(t, res)
	require.Equal(t, "test1-abc", invoked)
	res, e = httputil.Get(ctx, fmt.Sprintf("%s/test1/bcd", r.url))
	require.Nil(t, e)
	require.NotNil(t, res)
	require.Equal(t, "test1-param", invoked)
	res, e = httputil.Get(ctx, fmt.Sprintf("%s/not-found", r.url))
	require.Nil(t, e)
	require.Equal(t, http.StatusNotFound, res.StatusCode)
	res, e = httputil.Get(ctx, fmt.Sprintf("%s/test-skip", r.url))
	require.Nil(t, e)
	require.Equal(t, http.StatusNotFound, res.StatusCode)
}

type resource struct {
	wg     *sync.WaitGroup
	url    string
	server *http.Server
}

func setup(t *testing.T) (*resource, error) {
	port, err := getFreePort()
	if err != nil {
		return nil, err
	}
	r := &resource{}
	r.wg = &sync.WaitGroup{}
	r.wg.Add(1)
	r.server = &http.Server{Addr: fmt.Sprintf(":%d", port)}
	r.url = fmt.Sprintf("http://localhost:%d", port)
	go func() {
		defer r.wg.Done()
		t.Logf("starting http server on port %d", port)
		err = r.server.ListenAndServe()
	}()
	return r, nil
}

func teardown(ctx context.Context, t *testing.T, r *resource) error {
	if err := r.server.Shutdown(ctx); err != nil {
		return err
	}
	r.wg.Wait()
	t.Log("http server stopped.")
	return nil
}

func getFreePort() (port int, err error) {
	var a *net.TCPAddr
	if a, err = net.ResolveTCPAddr("tcp", "localhost:0"); err == nil {
		var l *net.TCPListener
		if l, err = net.ListenTCP("tcp", a); err == nil {
			defer l.Close()
			return l.Addr().(*net.TCPAddr).Port, nil
		}
	}
	return
}

type MockRegistry struct {
	mockReg []*URLRegistry
}

func (mr *MockRegistry) GetRegistrations(_ context.Context) []*URLRegistry {
	return mr.mockReg
}

func setupMockRegistry(mockReg []*URLRegistry) Registry {
	return &MockRegistry{mockReg: mockReg}
}
