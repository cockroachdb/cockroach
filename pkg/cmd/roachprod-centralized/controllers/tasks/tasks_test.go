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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	tasksmodel "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks"
	taskssmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/mocks"
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

	mockTasksService := taskssmock.NewIService(t)

	testCases := []testCase{
		{
			name: "get all tasks",
			request: request{
				method: "GET",
				url:    "/tasks",
			},
			serviceResult: serviceResult{},
			expected: expected{
				code:      http.StatusOK,
				arguments: (&InputGetAllDTO{}).ToServiceInputGetAllDTO(),
			},
		},
		{
			name: "get all tasks with type and state",
			request: request{
				method: "GET",
				url:    "/tasks?type=test&state=running",
			},
			serviceResult: serviceResult{},
			expected: expected{
				code:      http.StatusOK,
				arguments: (&InputGetAllDTO{Type: "test", State: "running"}).ToServiceInputGetAllDTO(),
			},
		},
		{
			name: "get all tasks with invalid type",
			request: request{
				method: "GET",
				url:    "/tasks?type=@_!invalid",
			},
			serviceResult: serviceResult{},
			expected: expected{
				code:          http.StatusBadRequest,
				bodyMatch:     "Field validation for 'Type' failed on the 'alphanum' tag",
				noMockedCalls: true,
			},
		},
		{
			name: "get all tasks with invalid state",
			request: request{
				method: "GET",
				url:    "/tasks?state=invalid",
			},
			serviceResult: serviceResult{},
			expected: expected{
				code:          http.StatusBadRequest,
				bodyMatch:     "Field validation for 'State' failed on the 'oneof' tag",
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
					tc.expected.arguments,
				).Return(tc.serviceResult.val, tc.serviceResult.err).Once()
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
				url:    "/tasks/2aaa68ee-c2aa-43d3-8b64-e602a7c99748",
			},
			serviceResult: serviceResult{
				val: &tasksmodel.Task{
					ID: uuid.FromStringOrNil("2aaa68ee-c2aa-43d3-8b64-e602a7c99748"),
				},
				err: nil,
			},
			expected: expected{
				code: http.StatusOK,
				arguments: tasks.InputGetTaskDTO{
					ID: uuid.FromStringOrNil("2aaa68ee-c2aa-43d3-8b64-e602a7c99748"),
				},
			},
		},
		{
			name: "get one task invalid id",
			request: request{
				method: "GET",
				url:    "/tasks/test",
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
				url:    "/tasks/2aaa68ee-c2aa-43d3-8b64-e602a7c99748",
			},
			serviceResult: serviceResult{
				val: nil,
				err: tasks.ErrTaskNotFound,
			},
			expected: expected{
				code:      http.StatusNotFound,
				bodyMatch: "task not found",
				arguments: tasks.InputGetTaskDTO{
					ID: uuid.FromStringOrNil("2aaa68ee-c2aa-43d3-8b64-e602a7c99748"),
				},
			},
		},
		{
			name: "get one task private internal error not exposed",
			request: request{
				method: "GET",
				url:    "/tasks/2aaa68ee-c2aa-43d3-8b64-e602a7c99748",
			},
			serviceResult: serviceResult{
				val: nil,
				err: errors.New("private internal error"),
			},
			expected: expected{
				code:      http.StatusInternalServerError,
				bodyMatch: controllers.ErrInternalServerError.Error(),
				arguments: tasks.InputGetTaskDTO{
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
				app.WithApiAuthenticationDisabled(true),
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
