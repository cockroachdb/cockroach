// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/app"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters"
	clustersmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/mocks"
	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type testCase struct {
	name          string
	request       request
	serviceResult serviceResult
	expected      expected
}
type request struct {
	method string
	url    string
	body   string
}
type serviceResult struct {
	val interface{}
	err error
}
type expected struct {
	code          int
	bodyMatch     string
	arguments     interface{}
	noMockedCalls bool
}

func TestGetAll(t *testing.T) {

	// Init mock users repo
	mockClustersService := clustersmock.NewIService(t)

	testCases := []testCase{
		{
			name: "get all clusters",
			request: request{
				method: "GET",
				url:    "/clusters",
			},
			serviceResult: serviceResult{},
			expected: expected{
				code:      http.StatusOK,
				arguments: (&InputGetAllDTO{}).ToServiceInputGetAllDTO(),
			},
		},
		{
			name: "get all clusters with username",
			request: request{
				method: "GET",
				url:    "/clusters?username=johndoe",
			},
			serviceResult: serviceResult{},
			expected: expected{
				code:      http.StatusOK,
				arguments: (&InputGetAllDTO{Username: "johndoe"}).ToServiceInputGetAllDTO(),
			},
		},
		{
			name: "get all clusters with wrong argument",
			request: request{
				method: "GET",
				url:    "/clusters?wrongarg=johndoe",
			},
			serviceResult: serviceResult{},
			expected: expected{
				code:      http.StatusOK,
				arguments: (&InputGetAllDTO{}).ToServiceInputGetAllDTO(),
			},
		},
	}

	for _, tc := range testCases {

		t.Run(tc.name, func(t *testing.T) {

			w := httptest.NewRecorder()
			c, e := gin.CreateTestContext(w)

			app, err := app.NewApp(
				app.WithApiAuthenticationDisabled(true),
				app.WithApiGinEngine(e),
				app.WithApiController(NewController(mockClustersService)),
			)
			assert.NoError(t, err)

			bodyReader := strings.NewReader(tc.request.body)
			req, _ := http.NewRequestWithContext(c, tc.request.method, tc.request.url, bodyReader)

			if !tc.expected.noMockedCalls {
				mockClustersService.On(
					"GetAllClusters",
					c,
					mock.Anything,
					tc.expected.arguments,
				).Return(tc.serviceResult.val, tc.serviceResult.err).Once()
			}

			app.GetApi().GetGinEngine().ServeHTTP(w, req)
			assert.Equal(t, tc.expected.code, w.Code)
			assert.Contains(t, w.Body.String(), tc.expected.bodyMatch)

			// Assert the mock service calls and reset
			mockClustersService.AssertExpectations(t)
			mockClustersService.ExpectedCalls = nil

		})
	}
}
func TestGetOne(t *testing.T) {
	mockClustersService := clustersmock.NewIService(t)

	testCases := []testCase{
		{
			name: "get one cluster",
			request: request{
				method: "GET",
				url:    "/clusters/test-cluster",
			},
			serviceResult: serviceResult{
				val: &cloud.Cluster{Name: "test-cluster"},
				err: nil,
			},
			expected: expected{
				code:      http.StatusOK,
				arguments: clusters.InputGetClusterDTO{Name: "test-cluster"},
			},
		},
		{
			name: "get one cluster not found",
			request: request{
				method: "GET",
				url:    "/clusters/not-found",
			},
			serviceResult: serviceResult{
				val: nil,
				err: clusters.ErrClusterNotFound,
			},
			expected: expected{
				code:      http.StatusNotFound,
				bodyMatch: "cluster not found",
				arguments: clusters.InputGetClusterDTO{Name: "not-found"},
			},
		},
		{
			name: "get one cluster private internal error not exposed",
			request: request{
				method: "GET",
				url:    "/clusters/internal-error",
			},
			serviceResult: serviceResult{
				val: nil,
				err: errors.New("private internal error"),
			},
			expected: expected{
				code:      http.StatusInternalServerError,
				bodyMatch: controllers.ErrInternalServerError.Error(),
				arguments: clusters.InputGetClusterDTO{Name: "internal-error"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			w := httptest.NewRecorder()
			c, e := gin.CreateTestContext(w)

			app, err := app.NewApp(
				app.WithApiAuthenticationDisabled(true),
				app.WithApiGinEngine(e),
				app.WithApiController(NewController(mockClustersService)),
			)
			assert.NoError(t, err)

			bodyReader := strings.NewReader(tc.request.body)
			req, _ := http.NewRequestWithContext(c, tc.request.method, tc.request.url, bodyReader)

			if !tc.expected.noMockedCalls {
				mockClustersService.On(
					"GetCluster",
					c,
					mock.Anything,
					tc.expected.arguments,
				).Return(tc.serviceResult.val, tc.serviceResult.err).Once()
			}

			app.GetApi().GetGinEngine().ServeHTTP(w, req)
			assert.Equal(t, tc.expected.code, w.Code)
			assert.Contains(t, w.Body.String(), tc.expected.bodyMatch)

			mockClustersService.AssertExpectations(t)
			mockClustersService.ExpectedCalls = nil
		})
	}
}

func TestCreate(t *testing.T) {
	mockClustersService := clustersmock.NewIService(t)

	testCases := []testCase{
		{
			name: "create cluster success",
			request: request{
				method: "POST",
				url:    "/clusters",
				body: `{
					"name":"test-cluster",
					"operation-begin-time": "2021-01-01T00:00:00Z",
					"operation-end-time": "2021-01-01T00:00:00Z"
				}`,
			},
			serviceResult: serviceResult{
				val: &cloud.Cluster{Name: "test-cluster"},
				err: nil,
			},
			expected: expected{
				arguments: mock.Anything,
				code:      http.StatusOK,
				bodyMatch: "test-cluster",
			},
		},
		{
			name: "create cluster invalid json",
			request: request{
				method: "POST",
				url:    "/clusters",
				body:   `{invalid json}`,
			},
			expected: expected{
				code:          http.StatusBadRequest,
				bodyMatch:     "invalid character",
				noMockedCalls: true,
			},
		},
		{
			name: "create cluster missing operation-end-time",
			request: request{
				method: "POST",
				url:    "/clusters",
				body: `{
					"name":"test-cluster",
					"operation-begin-time": "2021-01-01T00:00:00Z"
				}`,
			},
			expected: expected{
				code:          http.StatusBadRequest,
				bodyMatch:     "failed on the 'required' tag",
				noMockedCalls: true,
			},
		},
		{
			name: "create cluster missing operation-begin-time",
			request: request{
				method: "POST",
				url:    "/clusters",
				body: `{
					"name":"test-cluster",
					"operation-end-time": "2021-01-01T00:00:00Z"
				}`,
			},
			expected: expected{
				code:          http.StatusBadRequest,
				bodyMatch:     "failed on the 'required' tag",
				noMockedCalls: true,
			},
		},
		{
			name: "create cluster invalid operation-begin-time",
			request: request{
				method: "POST",
				url:    "/clusters",
				body: `{
					"name":"test-cluster",
					"operation-begin-time": "2021-13-32T00:00:00Z",
					"operation-end-time": "2021-01-01T00:00:00Z"
				}`,
			},
			expected: expected{
				code:          http.StatusBadRequest,
				bodyMatch:     "month out of range",
				noMockedCalls: true,
			},
		},
		{
			name: "create cluster already exists",
			request: request{
				method: "POST",
				url:    "/clusters",
				body: `{
					"name":"test-cluster",
					"operation-begin-time": "2021-01-01T00:00:00Z",
					"operation-end-time": "2021-01-01T00:00:00Z"
				}`,
			},
			serviceResult: serviceResult{
				val: nil,
				err: clusters.ErrClusterAlreadyExists,
			},
			expected: expected{
				arguments: mock.Anything,
				code:      http.StatusConflict,
				bodyMatch: "cluster already exists",
			},
		},
		{
			name: "create cluster internal error",
			request: request{
				method: "POST",
				url:    "/clusters",
				body: `{
					"name":"test-cluster",
					"operation-begin-time": "2021-01-01T00:00:00Z",
					"operation-end-time": "2021-01-01T00:00:00Z"
				}`,
			},
			serviceResult: serviceResult{
				val: nil,
				err: errors.New("private internal error"),
			},
			expected: expected{
				arguments: mock.Anything,
				code:      http.StatusInternalServerError,
				bodyMatch: controllers.ErrInternalServerError.Error(),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			w := httptest.NewRecorder()
			c, e := gin.CreateTestContext(w)

			app, err := app.NewApp(
				app.WithApiAuthenticationDisabled(true),
				app.WithApiGinEngine(e),
				app.WithApiController(NewController(mockClustersService)),
			)
			assert.NoError(t, err)

			bodyReader := strings.NewReader(tc.request.body)
			req, _ := http.NewRequestWithContext(c, tc.request.method, tc.request.url, bodyReader)

			if !tc.expected.noMockedCalls {
				mockClustersService.On(
					"CreateCluster",
					mock.Anything,
					mock.Anything,
					tc.expected.arguments,
				).Return(tc.serviceResult.val, tc.serviceResult.err).Once()
			}

			app.GetApi().GetGinEngine().ServeHTTP(w, req)
			assert.Equal(t, tc.expected.code, w.Code)
			assert.Contains(t, w.Body.String(), tc.expected.bodyMatch)

			mockClustersService.AssertExpectations(t)
			mockClustersService.ExpectedCalls = nil
		})
	}
}

func TestUpdate(t *testing.T) {
	mockClustersService := clustersmock.NewIService(t)

	testCases := []testCase{
		{
			name: "update cluster success",
			request: request{
				method: "PUT",
				url:    "/clusters/test-cluster",
				body: `{
					"name":"test-cluster",
					"operation-begin-time": "2021-01-01T00:00:00Z",
					"operation-end-time": "2021-01-01T00:00:00Z"
				}`,
			},
			serviceResult: serviceResult{
				val: &cloud.Cluster{Name: "test-cluster"},
				err: nil,
			},
			expected: expected{
				arguments: mock.Anything,
				code:      http.StatusOK,
				bodyMatch: "test-cluster",
			},
		},
		{
			name: "update cluster invalid json",
			request: request{
				method: "PUT",
				url:    "/clusters/test-cluster",
				body:   `{invalid json}`,
			},
			expected: expected{
				code:          http.StatusBadRequest,
				noMockedCalls: true,
				bodyMatch:     "invalid character",
			},
		},
		{
			name: "update cluster missing operation-end-time",
			request: request{
				method: "PUT",
				url:    "/clusters/test-cluster",
				body: `{
					"name":"test-cluster",
					"operation-begin-time": "2021-01-01T00:00:00Z"
				}`,
			},
			expected: expected{
				code:          http.StatusBadRequest,
				noMockedCalls: true,
				bodyMatch:     "failed on the 'required' tag",
			},
		},
		{
			name: "update cluster missing operation-begin-time",
			request: request{
				method: "PUT",
				url:    "/clusters/test-cluster",
				body: `{
					"name":"test-cluster",
					"operation-end-time": "2021-01-01T00:00:00Z"
				}`,
			},
			expected: expected{
				code:          http.StatusBadRequest,
				noMockedCalls: true,
				bodyMatch:     "failed on the 'required' tag",
			},
		},
		{
			name: "update cluster invalid operation-begin-time",
			request: request{
				method: "PUT",
				url:    "/clusters/test-cluster",
				body: `{
					"name":"test-cluster",
					"operation-begin-time": "2021-13-32T00:00:00Z",
					"operation-end-time": "2021-01-01T00:00:00Z"
				}`,
			},
			expected: expected{
				code:          http.StatusBadRequest,
				noMockedCalls: true,
				bodyMatch:     "month out of range",
			},
		},
		{
			name: "update cluster not exists",
			request: request{
				method: "PUT",
				url:    "/clusters/test-cluster",
				body: `{
					"name":"test-cluster",
					"operation-begin-time": "2021-01-01T00:00:00Z",
					"operation-end-time": "2021-01-01T00:00:00Z"
				}`,
			},
			serviceResult: serviceResult{
				val: nil,
				err: clusters.ErrClusterNotFound,
			},
			expected: expected{
				arguments: mock.Anything,
				code:      http.StatusNotFound,
				bodyMatch: "cluster not found",
			},
		},
		{
			name: "update cluster internal error",
			request: request{
				method: "PUT",
				url:    "/clusters/test-cluster",
				body: `{
					"name":"test-cluster",
					"operation-begin-time": "2021-01-01T00:00:00Z",
					"operation-end-time": "2021-01-01T00:00:00Z"
				}`,
			},
			serviceResult: serviceResult{
				val: nil,
				err: errors.New("private internal error"),
			},
			expected: expected{
				arguments: mock.Anything,
				code:      http.StatusInternalServerError,
				bodyMatch: controllers.ErrInternalServerError.Error(),
			},
		},
		{
			name: "update wrong cluster",
			request: request{
				method: "PUT",
				url:    "/clusters/test-cluster",
				body: `{
					"name":"other-cluster",
					"operation-begin-time": "2021-01-01T00:00:00Z",
					"operation-end-time": "2021-01-01T00:00:00Z"
				}`,
			},
			expected: expected{
				code:          http.StatusBadRequest,
				bodyMatch:     ErrWrongClusterName.Error(),
				noMockedCalls: true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, e := gin.CreateTestContext(w)

			app, err := app.NewApp(
				app.WithApiAuthenticationDisabled(true),
				app.WithApiGinEngine(e),
				app.WithApiController(NewController(mockClustersService)),
			)
			assert.NoError(t, err)

			bodyReader := strings.NewReader(tc.request.body)
			req, _ := http.NewRequestWithContext(c, tc.request.method, tc.request.url, bodyReader)

			if !tc.expected.noMockedCalls {
				mockClustersService.On(
					"UpdateCluster",
					mock.Anything,
					mock.Anything,
					tc.expected.arguments,
				).Return(tc.serviceResult.val, tc.serviceResult.err).Once()
			}

			app.GetApi().GetGinEngine().ServeHTTP(w, req)
			assert.Equal(t, tc.expected.code, w.Code)
			assert.Contains(t, w.Body.String(), tc.expected.bodyMatch)

			mockClustersService.AssertExpectations(t)
			mockClustersService.ExpectedCalls = nil
		})
	}
}

func TestDelete(t *testing.T) {
	mockClustersService := clustersmock.NewIService(t)

	testCases := []testCase{
		{
			name: "delete cluster",
			request: request{
				method: "DELETE",
				url:    "/clusters/test-cluster",
			},
			expected: expected{
				code:      http.StatusOK,
				arguments: clusters.InputDeleteClusterDTO{Name: "test-cluster"},
			},
		},
		{
			name: "delete cluster not found",
			request: request{
				method: "DELETE",
				url:    "/clusters/test-cluster",
			},
			serviceResult: serviceResult{
				err: clusters.ErrClusterNotFound,
			},
			expected: expected{
				code:      http.StatusNotFound,
				arguments: clusters.InputDeleteClusterDTO{Name: "test-cluster"},
				bodyMatch: "cluster not found",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, e := gin.CreateTestContext(w)

			app, err := app.NewApp(
				app.WithApiAuthenticationDisabled(true),
				app.WithApiGinEngine(e),
				app.WithApiController(NewController(mockClustersService)),
			)
			assert.NoError(t, err)

			bodyReader := strings.NewReader(tc.request.body)
			req, _ := http.NewRequestWithContext(c, tc.request.method, tc.request.url, bodyReader)

			if !tc.expected.noMockedCalls {
				mockClustersService.On(
					"DeleteCluster",
					c,
					mock.Anything,
					tc.expected.arguments,
				).Return(tc.serviceResult.err).Once()
			}

			app.GetApi().GetGinEngine().ServeHTTP(w, req)
			assert.Equal(t, tc.expected.code, w.Code)
			assert.Contains(t, w.Body.String(), tc.expected.bodyMatch)

			mockClustersService.AssertExpectations(t)
			mockClustersService.ExpectedCalls = nil
		})
	}
}

func TestSync(t *testing.T) {
	mockClustersService := clustersmock.NewIService(t)

	testCases := []testCase{
		{
			name: "sync clusters",
			request: request{
				method: "POST",
				url:    "/clusters/sync",
			},
			expected: expected{
				code: http.StatusOK,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			w := httptest.NewRecorder()
			c, e := gin.CreateTestContext(w)

			app, err := app.NewApp(
				app.WithApiAuthenticationDisabled(true),
				app.WithApiGinEngine(e),
				app.WithApiController(NewController(mockClustersService)),
			)
			assert.NoError(t, err)

			bodyReader := strings.NewReader(tc.request.body)
			req, _ := http.NewRequestWithContext(c, tc.request.method, tc.request.url, bodyReader)

			if !tc.expected.noMockedCalls {
				mockClustersService.On(
					"SyncClouds",
					c,
					mock.Anything,
				).Return(tc.serviceResult.val, tc.serviceResult.err).Once()
			}

			app.GetApi().GetGinEngine().ServeHTTP(w, req)
			assert.Equal(t, tc.expected.code, w.Code)
			assert.Contains(t, w.Body.String(), tc.expected.bodyMatch)

			mockClustersService.AssertExpectations(t)
			mockClustersService.ExpectedCalls = nil
		})
	}
}
