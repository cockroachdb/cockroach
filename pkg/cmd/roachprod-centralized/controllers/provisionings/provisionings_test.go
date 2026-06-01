// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisionings

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/app"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth/disabled"
	provtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/provisionings/types"
	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	provisioningsmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/mocks"
	serviceprovtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/types"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestCreate_ReturnsTaskID(t *testing.T) {
	mockService := provisioningsmock.NewIService(t)

	taskID := uuid.MakeV4()
	provID := uuid.MakeV4()
	prov := provmodels.Provisioning{
		ID:          provID,
		Environment: "env-a",
		State:       provmodels.ProvisioningStateNew,
	}

	w := httptest.NewRecorder()
	c, e := gin.CreateTestContext(w)

	apiApp, err := app.NewApp(
		app.WithApiAuthenticator(disabled.NewDisabledAuthenticator()),
		app.WithApiGinEngine(e),
		app.WithApiController(NewController(mockService)),
	)
	assert.NoError(t, err)

	mockService.On(
		"CreateProvisioning",
		c,
		mock.Anything,
		mock.AnythingOfType("*auth.Principal"),
		mock.MatchedBy(func(input serviceprovtypes.InputCreateDTO) bool {
			return input.Environment == "env-a" &&
				input.TemplateType == "tmpl-meta" &&
				input.Lifetime == "1h" &&
				input.Variables["region"] == "us-east1"
		}),
	).Return(prov, &taskID, nil).Once()

	reqBody := `{
		"environment":"env-a",
		"template_type":"tmpl-meta",
		"variables":{"region":"us-east1"},
		"lifetime":"1h"
	}`
	req, _ := http.NewRequestWithContext(c, http.MethodPost, provtypes.ControllerPath, strings.NewReader(reqBody))
	apiApp.GetApi().GetGinEngine().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"task_id":"`+taskID.String()+`"`)
	assert.Contains(t, w.Body.String(), `"id":"`+provID.String()+`"`)
}

func TestDestroy_InvalidProvisioningID(t *testing.T) {
	mockService := provisioningsmock.NewIService(t)
	w := httptest.NewRecorder()
	c, e := gin.CreateTestContext(w)

	apiApp, err := app.NewApp(
		app.WithApiAuthenticator(disabled.NewDisabledAuthenticator()),
		app.WithApiGinEngine(e),
		app.WithApiController(NewController(mockService)),
	)
	assert.NoError(t, err)

	req, _ := http.NewRequestWithContext(c, http.MethodPost, provtypes.ControllerPath+"/not-a-uuid/destroy", nil)
	apiApp.GetApi().GetGinEngine().ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid provisioning ID")
	mockService.AssertNotCalled(t, "DestroyProvisioning", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestDestroy_OmitsTaskIDWhenNoTaskCreated(t *testing.T) {
	mockService := provisioningsmock.NewIService(t)
	provID := uuid.MakeV4()
	prov := provmodels.Provisioning{
		ID:          provID,
		Environment: "env-a",
		State:       provmodels.ProvisioningStateDestroyed,
	}

	w := httptest.NewRecorder()
	c, e := gin.CreateTestContext(w)

	apiApp, err := app.NewApp(
		app.WithApiAuthenticator(disabled.NewDisabledAuthenticator()),
		app.WithApiGinEngine(e),
		app.WithApiController(NewController(mockService)),
	)
	assert.NoError(t, err)

	mockService.On(
		"DestroyProvisioning",
		c,
		mock.Anything,
		mock.AnythingOfType("*auth.Principal"),
		provID,
	).Return(prov, nil, nil).Once()

	req, _ := http.NewRequestWithContext(c, http.MethodPost, provtypes.ControllerPath+"/"+provID.String()+"/destroy", nil)
	apiApp.GetApi().GetGinEngine().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.NotContains(t, w.Body.String(), `"task_id"`)
	assert.Contains(t, w.Body.String(), `"id":"`+provID.String()+`"`)
}
