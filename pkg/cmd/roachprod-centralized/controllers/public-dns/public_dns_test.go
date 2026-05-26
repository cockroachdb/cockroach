// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package publicdns

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/app"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth/disabled"
	publicdnsmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/public-dns/mocks"
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
	noMockedCalls bool
}

func TestSync(t *testing.T) {
	mockPublicDNSService := publicdnsmock.NewIService(t)

	testCases := []testCase{
		{
			name: "sync DNS",
			request: request{
				method: "POST",
				url:    "/v1/public-dns/sync",
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
				app.WithApiController(NewController(mockPublicDNSService)),
			)
			assert.NoError(t, err)

			bodyReader := strings.NewReader(tc.request.body)
			req, _ := http.NewRequestWithContext(c, tc.request.method, tc.request.url, bodyReader)

			if !tc.expected.noMockedCalls {
				mockPublicDNSService.On(
					"SyncDNS",
					c,
					mock.Anything,
				).Return(tc.serviceResult.val, tc.serviceResult.err).Once()
			}

			app.GetApi().GetGinEngine().ServeHTTP(w, req)
			assert.Equal(t, tc.expected.code, w.Code)
			assert.Contains(t, w.Body.String(), tc.expected.bodyMatch)

			mockPublicDNSService.AssertExpectations(t)
			mockPublicDNSService.ExpectedCalls = nil
		})
	}
}
