// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/auth/types"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/gin-gonic/gin"
)

// Controller handles authentication-related HTTP requests.
type Controller struct {
	*controllers.Controller
	authService authtypes.IService
	handlers    []controllers.IControllerHandler
}

// NewController creates a new authentication controller.
func NewController(authService authtypes.IService) *Controller {
	ctrl := &Controller{
		Controller:  controllers.NewDefaultController(),
		authService: authService,
	}
	ctrl.handlers = []controllers.IControllerHandler{
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath + "/whoami",
			Func:   ctrl.WhoAmI,
		},
	}
	return ctrl
}

// GetControllerHandlers returns the controller's handlers.
func (ctrl *Controller) GetControllerHandlers() []controllers.IControllerHandler {
	return ctrl.handlers
}

// WhoAmI returns information about the current authenticated principal.
// GET /api/v1/auth/whoami
func (ctrl *Controller) WhoAmI(c *gin.Context) {
	// Extract principal from context (set by bearer authenticator)
	principal, exists := controllers.GetPrincipal(c)
	if !exists {
		ctrl.Render(c, types.NewAuthResult(nil, authtypes.ErrNotAuthenticated))
		return
	}

	ctrl.Render(c, types.NewAuthResult(types.BuildWhoAmIResponse(principal), nil))
}
