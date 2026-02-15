// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/tasks/types"
	stypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/bindings/stripe"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/query"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/gin-gonic/gin"
)

// Controller is the tasks controller.
type Controller struct {
	*controllers.Controller
	service  stypes.IService
	handlers []controllers.IControllerHandler
}

// NewController creates a new tasks controller.
func NewController(service stypes.IService) *Controller {
	ctrl := &Controller{
		Controller: controllers.NewDefaultController(),
		service:    service,
	}
	ctrl.handlers = []controllers.IControllerHandler{
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath,
			Func:   ctrl.GetAll,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					stypes.PermissionViewAll,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath + "/:id",
			Func:   ctrl.GetOne,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					stypes.PermissionViewAll,
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

// GetAll returns all tasks from the tasks service.
func (ctrl *Controller) GetAll(c *gin.Context) {

	principal, _ := controllers.GetPrincipal(c)

	var inputDTO types.InputGetAllDTO
	// Bind Stripe-style query parameters using our custom binding
	if err := c.ShouldBindWith(&inputDTO, stripe.StripeQuery); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	// Convert to FilterSet and add pagination/sorting
	filterSet := inputDTO.ToFilterSet()
	pagination, sort := query.ParseQueryParams(c, -1) // default: unlimited
	filterSet.Pagination = pagination
	filterSet.Sort = sort

	// Validate FilterSet
	if err := filterSet.Validate(); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	// Call service layer
	tasks, totalCount, err := ctrl.service.GetTasks(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
		stypes.InputGetAllTasksDTO{Filters: filterSet},
	)

	ctrl.Render(c, types.NewTasksListResult(tasks, totalCount, filterSet.Pagination, err))
}

// GetOne returns a task from the tasks service.
func (ctrl *Controller) GetOne(c *gin.Context) {

	principal, _ := controllers.GetPrincipal(c)

	id, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	task, err := ctrl.service.GetTask(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
		stypes.InputGetTaskDTO{
			ID: id,
		},
	)

	ctrl.Render(c, types.NewTaskResult(task, err))
}
