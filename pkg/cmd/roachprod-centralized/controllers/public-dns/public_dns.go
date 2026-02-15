// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package publicdns

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/public-dns/types"
	publicdns "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/public-dns/types"
	"github.com/gin-gonic/gin"
)

// Controller is the clusters controller.
type Controller struct {
	*controllers.Controller
	service  publicdns.IService
	handlers []controllers.IControllerHandler
}

// NewController creates a new clusters controller.
func NewController(service publicdns.IService) *Controller {
	ctrl := &Controller{
		Controller: controllers.NewDefaultController(),
		service:    service,
	}
	ctrl.handlers = []controllers.IControllerHandler{
		&controllers.ControllerHandler{
			Method: "POST",
			Path:   types.ControllerPath + "/sync",
			Func:   ctrl.Sync,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					publicdns.PermissionSync,
				},
			},
		},
	}
	return ctrl
}

// GetControllerHandlers returns the controller's handlers, as required
// by the controllers.IController interface.
func (ctrl *Controller) GetControllerHandlers() []controllers.IControllerHandler {
	return ctrl.handlers
}

// Sync triggers a clusters sync to the store.
func (ctrl *Controller) Sync(c *gin.Context) {
	task, err := ctrl.service.SyncDNS(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
	)

	ctrl.Render(c, types.NewTaskResult(task, err))
}
