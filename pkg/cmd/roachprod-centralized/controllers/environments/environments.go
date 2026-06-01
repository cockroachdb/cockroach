// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package environments

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/environments/types"
	envtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/types"
	"github.com/gin-gonic/gin"
)

// Controller is the environments HTTP controller.
type Controller struct {
	*controllers.Controller
	service  envtypes.IService
	handlers []controllers.IControllerHandler
}

// NewController creates a new environments controller.
func NewController(service envtypes.IService) *Controller {
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
					envtypes.PermissionViewAll,
					envtypes.PermissionViewOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath + "/:name",
			Func:   ctrl.GetOne,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					envtypes.PermissionViewAll,
					envtypes.PermissionViewOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "POST",
			Path:   types.ControllerPath,
			Func:   ctrl.Create,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					envtypes.PermissionCreate,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "PUT",
			Path:   types.ControllerPath + "/:name",
			Func:   ctrl.Update,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					envtypes.PermissionUpdateAll,
					envtypes.PermissionUpdateOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "DELETE",
			Path:   types.ControllerPath + "/:name",
			Func:   ctrl.Delete,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					envtypes.PermissionDeleteAll,
					envtypes.PermissionDeleteOwn,
				},
			},
		},
		// Variable sub-resource handlers
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath + "/:name/variables",
			Func:   ctrl.ListVariables,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					envtypes.PermissionViewAll,
					envtypes.PermissionViewOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath + "/:name/variables/:key",
			Func:   ctrl.GetVariable,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					envtypes.PermissionViewAll,
					envtypes.PermissionViewOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "POST",
			Path:   types.ControllerPath + "/:name/variables",
			Func:   ctrl.CreateVariable,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					envtypes.PermissionUpdateAll,
					envtypes.PermissionUpdateOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "PUT",
			Path:   types.ControllerPath + "/:name/variables/:key",
			Func:   ctrl.UpdateVariable,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					envtypes.PermissionUpdateAll,
					envtypes.PermissionUpdateOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "DELETE",
			Path:   types.ControllerPath + "/:name/variables/:key",
			Func:   ctrl.DeleteVariable,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					envtypes.PermissionUpdateAll,
					envtypes.PermissionUpdateOwn,
				},
			},
		},
	}
	return ctrl
}

// GetControllerHandlers returns the handlers for this controller.
func (ctrl *Controller) GetControllerHandlers() []controllers.IControllerHandler {
	return ctrl.handlers
}

// GetAll returns all environments.
func (ctrl *Controller) GetAll(c *gin.Context) {
	principal, _ := controllers.GetPrincipal(c)

	envs, err := ctrl.service.GetEnvironments(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
	)
	ctrl.Render(c, types.NewEnvironmentsResult(envs, err))
}

// GetOne returns a single environment by name.
func (ctrl *Controller) GetOne(c *gin.Context) {
	principal, _ := controllers.GetPrincipal(c)

	env, err := ctrl.service.GetEnvironment(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
		c.Param("name"),
	)
	if err != nil {
		ctrl.Render(c, types.NewEnvironmentResult(nil, err))
		return
	}
	ctrl.Render(c, types.NewEnvironmentResult(&env, nil))
}

// Create creates a new environment.
func (ctrl *Controller) Create(c *gin.Context) {
	principal, _ := controllers.GetPrincipal(c)

	var inputDTO types.InputCreateEnvironmentDTO
	if err := c.ShouldBindJSON(&inputDTO); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	env, err := ctrl.service.CreateEnvironment(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
		inputDTO.ToServiceDTO(),
	)
	if err != nil {
		ctrl.Render(c, types.NewEnvironmentResult(nil, err))
		return
	}
	ctrl.Render(c, types.NewEnvironmentResult(&env, nil))
}

// Update updates an existing environment.
func (ctrl *Controller) Update(c *gin.Context) {
	principal, _ := controllers.GetPrincipal(c)

	var inputDTO types.InputUpdateEnvironmentDTO
	if err := c.ShouldBindJSON(&inputDTO); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	env, err := ctrl.service.UpdateEnvironment(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
		c.Param("name"),
		inputDTO.ToServiceDTO(),
	)
	if err != nil {
		ctrl.Render(c, types.NewEnvironmentResult(nil, err))
		return
	}
	ctrl.Render(c, types.NewEnvironmentResult(&env, nil))
}

// Delete deletes an environment.
func (ctrl *Controller) Delete(c *gin.Context) {
	principal, _ := controllers.GetPrincipal(c)

	err := ctrl.service.DeleteEnvironment(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
		c.Param("name"),
	)
	ctrl.Render(c, types.NewDeleteResult(err))
}

// ListVariables returns all variables for an environment.
func (ctrl *Controller) ListVariables(c *gin.Context) {
	principal, _ := controllers.GetPrincipal(c)

	vars, err := ctrl.service.GetVariables(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
		c.Param("name"),
	)
	ctrl.Render(c, types.NewVariablesResult(vars, err))
}

// GetVariable returns a single variable.
func (ctrl *Controller) GetVariable(c *gin.Context) {
	principal, _ := controllers.GetPrincipal(c)

	v, err := ctrl.service.GetVariable(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
		c.Param("name"),
		c.Param("key"),
	)
	if err != nil {
		ctrl.Render(c, types.NewVariableResult(nil, err))
		return
	}
	ctrl.Render(c, types.NewVariableResult(&v, nil))
}

// CreateVariable creates a new variable on an environment.
func (ctrl *Controller) CreateVariable(c *gin.Context) {
	principal, _ := controllers.GetPrincipal(c)

	var inputDTO types.InputCreateVariableDTO
	if err := c.ShouldBindJSON(&inputDTO); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	v, err := ctrl.service.CreateVariable(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
		c.Param("name"),
		inputDTO.ToServiceDTO(),
	)
	if err != nil {
		ctrl.Render(c, types.NewVariableResult(nil, err))
		return
	}
	ctrl.Render(c, types.NewVariableResult(&v, nil))
}

// UpdateVariable updates a variable.
func (ctrl *Controller) UpdateVariable(c *gin.Context) {
	principal, _ := controllers.GetPrincipal(c)

	var inputDTO types.InputUpdateVariableDTO
	if err := c.ShouldBindJSON(&inputDTO); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	v, err := ctrl.service.UpdateVariable(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
		c.Param("name"),
		c.Param("key"),
		inputDTO.ToServiceDTO(),
	)
	if err != nil {
		ctrl.Render(c, types.NewVariableResult(nil, err))
		return
	}
	ctrl.Render(c, types.NewVariableResult(&v, nil))
}

// DeleteVariable deletes a variable.
func (ctrl *Controller) DeleteVariable(c *gin.Context) {
	principal, _ := controllers.GetPrincipal(c)

	err := ctrl.service.DeleteVariable(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
		c.Param("name"),
		c.Param("key"),
	)
	ctrl.Render(c, types.NewDeleteResult(err))
}
