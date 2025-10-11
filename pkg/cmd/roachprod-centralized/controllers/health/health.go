// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package health

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/gin-gonic/gin"
)

const (
	// ControllerPath is the path for the health controller.
	ControllerPath = "/v1/health"
)

// Controller is the health controller.
type Controller struct {
	*controllers.Controller
	handlers []controllers.IControllerHandler
}

// NewController creates a new health controller.
func NewController() (ctrl *Controller) {
	ctrl = &Controller{
		Controller: controllers.NewDefaultController(),
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

// GetHandlers returns the controller's handlers, as required
func (ctrl *Controller) GetHandlers() []controllers.IControllerHandler {
	return ctrl.handlers
}

// Ping returns pong
func (ctrl *Controller) Ping(c *gin.Context) {
	ctrl.Render(c, &HealthDTO{})
}
