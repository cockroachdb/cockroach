// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package health

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	healthtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/health/types"
	"github.com/gin-gonic/gin"
)

const (
	// ControllerPath is the path for the health controller.
	ControllerPath = "/v1/health"
)

// Controller is the health controller.
type Controller struct {
	*controllers.Controller
	service  healthtypes.IHealthService
	handlers []controllers.IControllerHandler
}

// NewController creates a new health controller.
func NewController(service healthtypes.IHealthService) (ctrl *Controller) {
	ctrl = &Controller{
		Controller: controllers.NewDefaultController(),
		service:    service,
	}
	ctrl.handlers = []controllers.IControllerHandler{
		&controllers.ControllerHandler{
			Method:         "GET",
			Path:           ControllerPath,
			Func:           ctrl.Ping,
			Authentication: controllers.AuthenticationTypeNone,
		},
	}
	return
}

// GetControllerHandlers returns the controller's handlers, as required
func (ctrl *Controller) GetControllerHandlers() []controllers.IControllerHandler {
	return ctrl.handlers
}

// Ping returns pong
func (ctrl *Controller) Ping(c *gin.Context) {
	ctrl.Render(c, &HealthDTO{})
}
