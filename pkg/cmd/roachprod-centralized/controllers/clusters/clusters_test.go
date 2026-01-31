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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth/disabled"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/clusters/types"
	clustersmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/mocks"
	clustermodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
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
				url:    "/v1/clusters",
			},
			serviceResult: serviceResult{},
			expected: expected{
				code:      http.StatusOK,
				arguments: clustermodels.NewInputGetAllClustersDTO(),
			},
		},
		{
			name: "get all clusters with username",
			request: request{
				method: "GET",
				url:    "/v1/clusters?name[like]=johndoe",
			},
			serviceResult: serviceResult{},
			expected: expected{
				code: http.StatusOK,
				arguments: clustermodels.InputGetAllClustersDTO{
					Filters: *filters.NewFilterSet().AddFilter("Name", filtertypes.OpLike, "johndoe"),
				},
			},
		},
		{
			name: "get all clusters with wrong argument",
			request: request{
				method: "GET",
				url:    "/v1/clusters?wrongarg=johndoe",
			},
			serviceResult: serviceResult{},
			expected: expected{
				code:          http.StatusBadRequest,
				bodyMatch:     "Field validation for 'wrongarg' failed on unknown field",
				noMockedCalls: true,
			},
		},
	}

	for _, tc := range testCases {

		t.Run(tc.name, func(t *testing.T) {

			w := httptest.NewRecorder()
			c, e := gin.CreateTestContext(w)

			app, err := app.NewApp(
				app.WithApiAuthenticator(disabled.NewDisabledAuthenticator()),
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
					mock.AnythingOfType("*auth.Principal"),
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
				url:    "/v1/clusters/test-cluster",
			},
			serviceResult: serviceResult{
				val: &cloudcluster.Cluster{Name: "test-cluster"},
				err: nil,
			},
			expected: expected{
				code:      http.StatusOK,
				arguments: clustermodels.InputGetClusterDTO{Name: "test-cluster"},
			},
		},
		{
			name: "get one cluster not found",
			request: request{
				method: "GET",
				url:    "/v1/clusters/not-found",
			},
			serviceResult: serviceResult{
				val: nil,
				err: clustermodels.ErrClusterNotFound,
			},
			expected: expected{
				code:      http.StatusNotFound,
				bodyMatch: "cluster not found",
				arguments: clustermodels.InputGetClusterDTO{Name: "not-found"},
			},
		},
		{
			name: "get one cluster private internal error not exposed",
			request: request{
				method: "GET",
				url:    "/v1/clusters/internal-error",
			},
			serviceResult: serviceResult{
				val: nil,
				err: errors.New("private internal error"),
			},
			expected: expected{
				code:      http.StatusInternalServerError,
				bodyMatch: controllers.ErrInternalServerError.Error(),
				arguments: clustermodels.InputGetClusterDTO{Name: "internal-error"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			w := httptest.NewRecorder()
			c, e := gin.CreateTestContext(w)

			app, err := app.NewApp(
				app.WithApiAuthenticator(disabled.NewDisabledAuthenticator()),
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
					mock.AnythingOfType("*auth.Principal"),
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

func TestRegister(t *testing.T) {
	mockClustersService := clustersmock.NewIService(t)

	testCases := []testCase{
		{
			name: "register cluster success",
			request: request{
				method: "POST",
				url:    "/v1/clusters/register",
				body: `{
					"name":"test-cluster"
				}`,
			},
			serviceResult: serviceResult{
				val: &cloudcluster.Cluster{Name: "test-cluster"},
				err: nil,
			},
			expected: expected{
				arguments: mock.Anything,
				code:      http.StatusOK,
				bodyMatch: "test-cluster",
			},
		},
		{
			name: "register cluster invalid json",
			request: request{
				method: "POST",
				url:    "/v1/clusters/register",
				body:   `{invalid json}`,
			},
			expected: expected{
				code:          http.StatusBadRequest,
				bodyMatch:     "invalid character",
				noMockedCalls: true,
			},
		},
		{
			name: "register cluster already exists",
			request: request{
				method: "POST",
				url:    "/v1/clusters/register",
				body: `{
					"name":"test-cluster"
				}`,
			},
			serviceResult: serviceResult{
				val: nil,
				err: clustermodels.ErrClusterAlreadyExists,
			},
			expected: expected{
				arguments: mock.Anything,
				code:      http.StatusConflict,
				bodyMatch: "cluster already exists",
			},
		},
		{
			name: "register cluster internal error",
			request: request{
				method: "POST",
				url:    "/v1/clusters/register",
				body: `{
					"name":"test-cluster"
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
				app.WithApiAuthenticator(disabled.NewDisabledAuthenticator()),
				app.WithApiGinEngine(e),
				app.WithApiController(NewController(mockClustersService)),
			)
			assert.NoError(t, err)

			bodyReader := strings.NewReader(tc.request.body)
			req, _ := http.NewRequestWithContext(c, tc.request.method, tc.request.url, bodyReader)

			if !tc.expected.noMockedCalls {
				mockClustersService.On(
					"RegisterCluster",
					mock.Anything,
					mock.Anything,
					mock.AnythingOfType("*auth.Principal"),
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

func TestRegisterUpdate(t *testing.T) {
	mockClustersService := clustersmock.NewIService(t)

	testCases := []testCase{
		{
			name: "register cluster update success",
			request: request{
				method: "PUT",
				url:    "/v1/clusters/register/test-cluster",
				body: `{
					"name":"test-cluster"
				}`,
			},
			serviceResult: serviceResult{
				val: &cloudcluster.Cluster{Name: "test-cluster"},
				err: nil,
			},
			expected: expected{
				arguments: mock.Anything,
				code:      http.StatusOK,
				bodyMatch: "test-cluster",
			},
		},
		{
			name: "register cluster update invalid json",
			request: request{
				method: "PUT",
				url:    "/v1/clusters/register/test-cluster",
				body:   `{invalid json}`,
			},
			expected: expected{
				code:          http.StatusBadRequest,
				noMockedCalls: true,
				bodyMatch:     "invalid character",
			},
		},
		{
			name: "register cluster update not exists",
			request: request{
				method: "PUT",
				url:    "/v1/clusters/register/test-cluster",
				body: `{
					"name":"test-cluster"
				}`,
			},
			serviceResult: serviceResult{
				val: nil,
				err: clustermodels.ErrClusterNotFound,
			},
			expected: expected{
				arguments: mock.Anything,
				code:      http.StatusNotFound,
				bodyMatch: "cluster not found",
			},
		},
		{
			name: "register cluster update internal error",
			request: request{
				method: "PUT",
				url:    "/v1/clusters/register/test-cluster",
				body: `{
					"name":"test-cluster"
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
			name: "register cluster update wrong cluster",
			request: request{
				method: "PUT",
				url:    "/v1/clusters/register/test-cluster",
				body: `{
					"name":"other-cluster"
				}`,
			},
			expected: expected{
				code:          http.StatusBadRequest,
				bodyMatch:     types.ErrWrongClusterName.Error(),
				noMockedCalls: true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, e := gin.CreateTestContext(w)

			app, err := app.NewApp(
				app.WithApiAuthenticator(disabled.NewDisabledAuthenticator()),
				app.WithApiGinEngine(e),
				app.WithApiController(NewController(mockClustersService)),
			)
			assert.NoError(t, err)

			bodyReader := strings.NewReader(tc.request.body)
			req, _ := http.NewRequestWithContext(c, tc.request.method, tc.request.url, bodyReader)

			if !tc.expected.noMockedCalls {
				mockClustersService.On(
					"RegisterClusterUpdate",
					mock.Anything,
					mock.Anything,
					mock.AnythingOfType("*auth.Principal"),
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

func TestRegisterDelete(t *testing.T) {
	mockClustersService := clustersmock.NewIService(t)

	testCases := []testCase{
		{
			name: "register cluster delete success",
			request: request{
				method: "DELETE",
				url:    "/v1/clusters/register/test-cluster",
			},
			expected: expected{
				code:      http.StatusOK,
				arguments: clustermodels.InputRegisterClusterDeleteDTO{Name: "test-cluster"},
			},
		},
		{
			name: "register cluster delete not found",
			request: request{
				method: "DELETE",
				url:    "/v1/clusters/register/test-cluster",
			},
			serviceResult: serviceResult{
				err: clustermodels.ErrClusterNotFound,
			},
			expected: expected{
				code:      http.StatusNotFound,
				arguments: clustermodels.InputRegisterClusterDeleteDTO{Name: "test-cluster"},
				bodyMatch: "cluster not found",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, e := gin.CreateTestContext(w)

			app, err := app.NewApp(
				app.WithApiAuthenticator(disabled.NewDisabledAuthenticator()),
				app.WithApiGinEngine(e),
				app.WithApiController(NewController(mockClustersService)),
			)
			assert.NoError(t, err)

			bodyReader := strings.NewReader(tc.request.body)
			req, _ := http.NewRequestWithContext(c, tc.request.method, tc.request.url, bodyReader)

			if !tc.expected.noMockedCalls {
				mockClustersService.On(
					"RegisterClusterDelete",
					c,
					mock.Anything,
					mock.AnythingOfType("*auth.Principal"),
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
				url:    "/v1/clusters/sync",
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
				app.WithApiAuthenticator(disabled.NewDisabledAuthenticator()),
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
					mock.AnythingOfType("*auth.Principal"),
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
