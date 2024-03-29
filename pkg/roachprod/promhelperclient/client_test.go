// Copyright 2024 The Cockroach Authors.

// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package promhelperclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateClusterConfig(t *testing.T) {
	ctx := context.Background()
	r, e := setup(t, fmt.Sprintf("/%s/%s", resourceVersion, resourceName))
	require.Nil(t, e)
	defer func() { _ = teardown(ctx, t, r) }()
	t.Run("CreateClusterConfig fails with 400", func(t *testing.T) {
		invoked := false
		r.handler = func(w http.ResponseWriter, r *http.Request) {
			invoked = true
			w.WriteHeader(400)
		}
		err := CreateClusterConfig(ctx, r.promUrl, "c1", []string{"n1"})
		require.NotNil(t, err)
		require.True(t, invoked)
	})
	t.Run("CreateClusterConfig succeeds with 201", func(t *testing.T) {
		invoked := false
		r.handler = func(w http.ResponseWriter, r *http.Request) {
			invoked = true
			ir, err := getInstanceConfigRequest(r.Body)
			require.Nil(t, err)
			require.Equal(t, http.MethodPost, r.Method)
			require.Equal(t, "c1", ir.ClusterID)
			require.Equal(t, "1", ir.Nodes[0].NodeID)
			require.Equal(t, "2", ir.Nodes[1].NodeID)
			require.Equal(t, "n1", ir.Nodes[0].Targets[0])
			require.Equal(t, "n2", ir.Nodes[1].Targets[0])
			require.Equal(t, "system", ir.Nodes[0].Labels["tenant"])
			require.Equal(t, "system", ir.Nodes[1].Labels["tenant"])
			w.WriteHeader(201)
		}
		err := CreateClusterConfig(ctx, r.promUrl, "c1", []string{"n1", "n2"})
		require.Nil(t, err)
		require.True(t, invoked)
	})
}

func TestDeleteClusterConfig(t *testing.T) {
	ctx := context.Background()
	r, e := setup(t, fmt.Sprintf("/%s/%s/c1", resourceVersion, resourceName))
	require.Nil(t, e)
	defer func() { _ = teardown(ctx, t, r) }()
	t.Run("DeleteClusterConfig fails with 400", func(t *testing.T) {
		invoked := false
		r.handler = func(w http.ResponseWriter, r *http.Request) {
			invoked = true
			w.WriteHeader(400)
		}
		err := DeleteClusterConfig(ctx, r.promUrl, "c1")
		require.NotNil(t, err)
		require.True(t, invoked)
	})
	t.Run("DeleteClusterConfig succeeds with 201", func(t *testing.T) {
		invoked := false
		r.handler = func(w http.ResponseWriter, r *http.Request) {
			invoked = true
			w.WriteHeader(204)
		}
		err := DeleteClusterConfig(ctx, r.promUrl, "c1")
		require.Nil(t, err)
		require.True(t, invoked)
	})
}

// getInstanceConfigRequest returns the instanceConfigRequest after parsing the request json
func getInstanceConfigRequest(body io.ReadCloser) (*instanceConfigRequest, error) {
	var insConfigReq instanceConfigRequest
	if err := json.NewDecoder(body).Decode(&insConfigReq); err != nil {
		return nil, err
	}
	return &insConfigReq, nil
}

type resource struct {
	promUrl string
	wg      *sync.WaitGroup
	server  *http.Server
	handler http.HandlerFunc
}

func setup(t *testing.T, resourceUrl string) (*resource, error) {
	port, err := getFreePort()
	if err != nil {
		return nil, err
	}
	r := &resource{}
	r.promUrl = fmt.Sprintf("http://localhost:%d", port)
	r.wg = &sync.WaitGroup{}
	r.wg.Add(1)
	r.server = &http.Server{Addr: fmt.Sprintf(":%d", port)}
	http.HandleFunc(resourceUrl, func(w http.ResponseWriter, req *http.Request) {
		r.handler(w, req)
	})
	go func() {
		defer r.wg.Done()
		t.Logf("starting http server as: %s", r.promUrl)
		_ = r.server.ListenAndServe()
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
			defer func() { _ = l.Close() }()
			return l.Addr().(*net.TCPAddr).Port, nil
		}
	}
	return
}
