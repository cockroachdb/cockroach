// Copyright 2024 The Cockroach Authors.
//
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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegisterHandlers(t *testing.T) {
	ctx := context.Background()
	invoked := ""
	mr := setupMockRegistry([]*URLRegistry{
		{
			Url:    "/test1",
			Method: GET,
			HandleFunc: func(w http.ResponseWriter, _ *http.Request) {
				invoked = "/test1-GET"
			},
		},
		{
			Url:    "/test2",
			Method: POST,
			HandleFunc: func(w http.ResponseWriter, _ *http.Request) {
				invoked = "/test2-POST"
			},
		},
		{
			Url:    "/test1",
			Method: POST,
			HandleFunc: func(w http.ResponseWriter, _ *http.Request) {
				invoked = "/test1-POST"
			},
		},
		{
			Url:    "/test1/abc",
			Method: GET,
			HandleFunc: func(w http.ResponseWriter, _ *http.Request) {
				invoked = "/test1/abc-GET"
			},
		},
		{
			Url:    "/test1/{param}",
			Method: GET,
			HandleFunc: func(w http.ResponseWriter, r *http.Request) {
				invoked = "/test1/{param}-GET"
			},
		},
		{
			Url:    "/test-skip",
			Method: GET,
			Skip:   true,
		},
	})
	urlProxyMap := make(map[string]http.HandlerFunc)
	httpHandlerFunc = func(pattern string, handler func(http.ResponseWriter, *http.Request)) {
		urlProxyMap[pattern] = handler
	}
	require.Nil(t, RegisterHandlers(ctx, mr))
	registeredMap := make(map[string]struct{})
	for _, r := range mr.mockReg {
		if !r.Skip {
			registeredMap[r.Url] = struct{}{}
		}
	}
	// -1 for the test-skip
	require.Equal(t, len(registeredMap), len(urlProxyMap))
	require.Empty(t, invoked)
	for _, r := range mr.mockReg {
		if !r.Skip {
			expectedInvoked := r.Url + "-" + string(r.Method)
			w := httptest.NewRecorder()
			urlProxyMap[r.Url](w, &http.Request{Method: string(r.Method)})
			require.Equal(t, expectedInvoked, invoked)
		} else {
			require.Nil(t, urlProxyMap[r.Url])
		}
	}
	w := httptest.NewRecorder()
	urlProxyMap["/test1/{param}"](w, &http.Request{Method: http.MethodPut})
	require.Equal(t, http.StatusNotFound, w.Code)
	mr.mockReg = append(mr.mockReg, &URLRegistry{
		Url:    "/test1",
		Method: GET,
		HandleFunc: func(w http.ResponseWriter, _ *http.Request) {
			invoked = "/test1-GET"
		},
	})
	require.NotNil(t, RegisterHandlers(ctx, mr))

}

type MockRegistry struct {
	mockReg []*URLRegistry
}

func (mr *MockRegistry) RegisterFlags() {
}

func (mr *MockRegistry) GetRegistrations(_ context.Context) []*URLRegistry {
	return mr.mockReg
}

func setupMockRegistry(mockReg []*URLRegistry) *MockRegistry {
	return &MockRegistry{mockReg: mockReg}
}
