// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/app"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth/disabled"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	tasksmodel "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	taskssmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/mocks"
	stypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	val        interface{}
	totalCount int
	err        error
}
type expected struct {
	code          int
	bodyMatch     string
	arguments     interface{}
	noMockedCalls bool
}

func TestGetAll(t *testing.T) {

	mockTasksService := taskssmock.NewIService(t)

	testCases := []testCase{
		{
			name: "get all tasks",
			request: request{
				method: "GET",
				url:    "/v1/tasks",
			},
			serviceResult: serviceResult{},
			expected: expected{
				code:      http.StatusOK,
				arguments: stypes.NewInputGetAllTasksDTO(),
			},
		},
		{
			name: "get all tasks with simple fields",
			request: request{
				method: "GET",
				url:    "/v1/tasks?type=test&state=running",
			},
			serviceResult: serviceResult{},
			expected: expected{
				code: http.StatusOK,
				arguments: stypes.InputGetAllTasksDTO{
					Filters: *filters.NewFilterSet().
						AddFilter("Type", filtertypes.OpEqual, "test").
						AddFilter("State", filtertypes.OpEqual, tasksmodel.TaskStateRunning),
				},
			},
		},
		{
			name: "get all tasks with invalid simple field",
			request: request{
				method: "GET",
				url:    "/v1/tasks?state=invalid",
			},
			serviceResult: serviceResult{},
			expected: expected{
				code:          http.StatusBadRequest,
				bodyMatch:     "Field validation for 'State' failed on the 'oneof' tag",
				noMockedCalls: true,
			},
		},
		{
			name: "get all tasks with unknown advanced field",
			request: request{
				method: "GET",
				url:    "/v1/tasks?unknown_field[gte]=2024-01-01",
			},
			serviceResult: serviceResult{},
			expected: expected{
				code:          http.StatusBadRequest,
				bodyMatch:     "Field validation for 'unknown_field' failed on unknown field",
				noMockedCalls: true,
			},
		},
		{
			name: "get all tasks with mixed simple and advanced filters",
			request: request{
				method: "GET",
				url:    "/v1/tasks?type=test&state[ne]=failed",
			},
			serviceResult: serviceResult{},
			expected: expected{
				code: http.StatusOK,
				arguments: stypes.InputGetAllTasksDTO{
					Filters: *filters.NewFilterSet().
						AddFilter("Type", filtertypes.OpEqual, "test").
						AddFilter("State", filtertypes.OpNotEqual, tasksmodel.TaskStateFailed),
				},
			},
		},
		{
			name: "get all tasks with invalid advanced filter",
			request: request{
				method: "GET",
				url:    "/v1/tasks?state[in]=invalid,failed",
			},
			serviceResult: serviceResult{},
			expected: expected{
				code:          http.StatusBadRequest,
				bodyMatch:     "Field validation for 'State' failed on the 'oneof' tag",
				noMockedCalls: true,
			},
		},
		{
			name: "get all tasks with oversized simple field",
			request: request{
				method: "GET",
				url:    "/v1/tasks?type=" + strings.Repeat("a", 51), // Exceed max=50 limit
			},
			serviceResult: serviceResult{},
			expected: expected{
				code:          http.StatusBadRequest,
				bodyMatch:     "Field validation for 'Type' failed on the 'max' tag",
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
				app.WithApiController(NewController(mockTasksService)),
			)
			assert.NoError(t, err)

			bodyReader := strings.NewReader(tc.request.body)
			req, _ := http.NewRequestWithContext(c, tc.request.method, tc.request.url, bodyReader)

			if !tc.expected.noMockedCalls {
				mockTasksService.On(
					"GetTasks",
					c,
					mock.Anything,
					mock.AnythingOfType("*auth.Principal"),
					tc.expected.arguments,
				).Return(tc.serviceResult.val, tc.serviceResult.totalCount, tc.serviceResult.err).Once()
			}

			app.GetApi().GetGinEngine().ServeHTTP(w, req)
			assert.Equal(t, tc.expected.code, w.Code)
			assert.Contains(t, w.Body.String(), tc.expected.bodyMatch)

			// Assert the mock service calls and reset
			mockTasksService.AssertExpectations(t)
			mockTasksService.ExpectedCalls = nil

		})
	}
}

func TestGetOne(t *testing.T) {

	mockTasksService := taskssmock.NewIService(t)

	testCases := []testCase{
		{
			name: "get one task",
			request: request{
				method: "GET",
				url:    "/v1/tasks/2aaa68ee-c2aa-43d3-8b64-e602a7c99748",
			},
			serviceResult: serviceResult{
				val: &tasksmodel.Task{
					ID: uuid.FromStringOrNil("2aaa68ee-c2aa-43d3-8b64-e602a7c99748"),
				},
				err: nil,
			},
			expected: expected{
				code: http.StatusOK,
				arguments: stypes.InputGetTaskDTO{
					ID: uuid.FromStringOrNil("2aaa68ee-c2aa-43d3-8b64-e602a7c99748"),
				},
			},
		},
		{
			name: "get one task invalid id",
			request: request{
				method: "GET",
				url:    "/v1/tasks/test",
			},
			expected: expected{
				code:          http.StatusBadRequest,
				bodyMatch:     "uuid: incorrect UUID",
				noMockedCalls: true,
			},
		},
		{
			name: "get one task not found",
			request: request{
				method: "GET",
				url:    "/v1/tasks/2aaa68ee-c2aa-43d3-8b64-e602a7c99748",
			},
			serviceResult: serviceResult{
				val: nil,
				err: stypes.ErrTaskNotFound,
			},
			expected: expected{
				code:      http.StatusNotFound,
				bodyMatch: "task not found",
				arguments: stypes.InputGetTaskDTO{
					ID: uuid.FromStringOrNil("2aaa68ee-c2aa-43d3-8b64-e602a7c99748"),
				},
			},
		},
		{
			name: "get one task private internal error not exposed",
			request: request{
				method: "GET",
				url:    "/v1/tasks/2aaa68ee-c2aa-43d3-8b64-e602a7c99748",
			},
			serviceResult: serviceResult{
				val: nil,
				err: errors.New("private internal error"),
			},
			expected: expected{
				code:      http.StatusInternalServerError,
				bodyMatch: controllers.ErrInternalServerError.Error(),
				arguments: stypes.InputGetTaskDTO{
					ID: uuid.FromStringOrNil("2aaa68ee-c2aa-43d3-8b64-e602a7c99748"),
				},
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
				app.WithApiController(NewController(mockTasksService)),
			)
			assert.NoError(t, err)

			bodyReader := strings.NewReader(tc.request.body)
			req, _ := http.NewRequestWithContext(c, tc.request.method, tc.request.url, bodyReader)

			if !tc.expected.noMockedCalls {
				mockTasksService.On(
					"GetTask",
					c,
					mock.Anything,
					mock.AnythingOfType("*auth.Principal"),
					tc.expected.arguments,
				).Return(tc.serviceResult.val, tc.serviceResult.err).Once()
			}

			app.GetApi().GetGinEngine().ServeHTTP(w, req)
			assert.Equal(t, tc.expected.code, w.Code)
			assert.Contains(t, w.Body.String(), tc.expected.bodyMatch)

			mockTasksService.AssertExpectations(t)
			mockTasksService.ExpectedCalls = nil
		})
	}
}
