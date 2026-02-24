// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisionings

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/provisionings/types"
	provtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/bindings/stripe"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/gin-gonic/gin"
)

// Controller is the provisionings HTTP controller.
type Controller struct {
	*controllers.Controller
	service  provtypes.IService
	handlers []controllers.IControllerHandler
}

// NewController creates a new provisionings controller.
func NewController(service provtypes.IService) *Controller {
	ctrl := &Controller{
		Controller: controllers.NewDefaultController(),
		service:    service,
	}
	ctrl.handlers = []controllers.IControllerHandler{
		// Templates (read-only)
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.TemplatesPath,
			Func:   ctrl.ListTemplates,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					provtypes.PermissionViewAll,
					provtypes.PermissionViewOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.TemplatesPath + "/:name",
			Func:   ctrl.GetTemplate,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					provtypes.PermissionViewAll,
					provtypes.PermissionViewOwn,
				},
			},
		},
		// Provisionings
		&controllers.ControllerHandler{
			Method: "POST",
			Path:   types.ControllerPath,
			Func:   ctrl.Create,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					provtypes.PermissionCreate,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath,
			Func:   ctrl.GetAll,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					provtypes.PermissionViewAll,
					provtypes.PermissionViewOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath + "/:id",
			Func:   ctrl.Get,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					provtypes.PermissionViewAll,
					provtypes.PermissionViewOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "POST",
			Path:   types.ControllerPath + "/:id/destroy",
			Func:   ctrl.Destroy,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					provtypes.PermissionDestroyAll,
					provtypes.PermissionDestroyOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "DELETE",
			Path:   types.ControllerPath + "/:id",
			Func:   ctrl.Delete,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					provtypes.PermissionDestroyAll,
					provtypes.PermissionDestroyOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath + "/:id/plan",
			Func:   ctrl.GetPlan,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					provtypes.PermissionViewAll,
					provtypes.PermissionViewOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath + "/:id/outputs",
			Func:   ctrl.GetOutputs,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					provtypes.PermissionViewAll,
					provtypes.PermissionViewOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "PATCH",
			Path:   types.ControllerPath + "/:id/lifetime",
			Func:   ctrl.ExtendLifetime,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					provtypes.PermissionUpdateAll,
					provtypes.PermissionUpdateOwn,
				},
			},
		},
		// TODO(golgeek): Consider per-environment permission gating for hook
		// endpoints. SSH key deployment may warrant environment-scoped
		// permissions in the future (e.g., provisionings:update:env:prod).
		&controllers.ControllerHandler{
			Method: "POST",
			Path:   types.ControllerPath + "/:id/hooks/ssh-keys-setup",
			Func:   ctrl.SetupSSHKeys,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					provtypes.PermissionUpdateAll,
					provtypes.PermissionUpdateOwn,
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

// ListTemplates returns all available templates.
func (ctrl *Controller) ListTemplates(c *gin.Context) {
	templates, err := ctrl.service.GetTemplates(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
	)
	ctrl.Render(c, types.NewTemplatesResult(templates, err))
}

// GetTemplate returns a single template by name.
func (ctrl *Controller) GetTemplate(c *gin.Context) {
	name := c.Param("name")
	tmpl, err := ctrl.service.GetTemplate(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		name,
	)
	if err != nil {
		ctrl.Render(c, types.NewTemplateResult(nil, err))
		return
	}
	ctrl.Render(c, types.NewTemplateResult(&tmpl, nil))
}

// Create creates a new provisioning.
func (ctrl *Controller) Create(c *gin.Context) {
	principal, _ := controllers.GetPrincipal(c)

	var inputDTO types.InputCreateProvisioningDTO
	if err := c.ShouldBindJSON(&inputDTO); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	prov, taskID, err := ctrl.service.CreateProvisioning(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
		inputDTO.ToServiceDTO(),
	)
	if err != nil {
		ctrl.Render(c, types.NewProvisioningResult(nil, nil, err))
		return
	}
	ctrl.Render(c, types.NewProvisioningResult(&prov, taskID, nil))
}

// GetAll returns all provisionings matching filter criteria.
func (ctrl *Controller) GetAll(c *gin.Context) {
	principal, _ := controllers.GetPrincipal(c)

	var inputDTO types.InputGetAllProvisioningsDTO
	if err := c.ShouldBindWith(&inputDTO, stripe.StripeQuery); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	provs, _, err := ctrl.service.GetProvisionings(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
		inputDTO.ToServiceDTO(),
	)
	ctrl.Render(c, types.NewProvisioningsResult(provs, err))
}

// Get returns a single provisioning by ID.
func (ctrl *Controller) Get(c *gin.Context) {
	principal, _ := controllers.GetPrincipal(c)

	id, err := parseUUID(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	prov, err := ctrl.service.GetProvisioning(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
		id,
	)
	if err != nil {
		ctrl.Render(c, types.NewProvisioningResult(nil, nil, err))
		return
	}
	ctrl.Render(c, types.NewProvisioningResult(&prov, nil, nil))
}

// Destroy initiates destruction of a provisioning.
func (ctrl *Controller) Destroy(c *gin.Context) {
	principal, _ := controllers.GetPrincipal(c)

	id, err := parseUUID(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	prov, taskID, err := ctrl.service.DestroyProvisioning(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
		id,
	)
	if err != nil {
		ctrl.Render(c, types.NewProvisioningResult(nil, nil, err))
		return
	}
	ctrl.Render(c, types.NewProvisioningResult(&prov, taskID, nil))
}

// Delete removes a provisioning record from the database.
// Only allowed when the provisioning is in state new or destroyed.
func (ctrl *Controller) Delete(c *gin.Context) {
	principal, _ := controllers.GetPrincipal(c)

	id, err := parseUUID(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	err = ctrl.service.DeleteProvisioning(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
		id,
	)
	ctrl.Render(c, types.NewDeleteResult(err))
}

// GetPlan returns the plan output for a provisioning.
func (ctrl *Controller) GetPlan(c *gin.Context) {
	principal, _ := controllers.GetPrincipal(c)

	id, err := parseUUID(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	plan, err := ctrl.service.GetProvisioningPlan(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
		id,
	)
	ctrl.Render(c, types.NewPlanResult(plan, err))
}

// GetOutputs returns the outputs for a provisioning.
func (ctrl *Controller) GetOutputs(c *gin.Context) {
	principal, _ := controllers.GetPrincipal(c)

	id, err := parseUUID(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	outputs, err := ctrl.service.GetProvisioningOutputs(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
		id,
	)
	ctrl.Render(c, types.NewOutputsResult(outputs, err))
}

// ExtendLifetime extends the expiration of a provisioning.
func (ctrl *Controller) ExtendLifetime(c *gin.Context) {
	principal, _ := controllers.GetPrincipal(c)

	id, err := parseUUID(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	prov, err := ctrl.service.ExtendLifetime(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
		id,
	)
	if err != nil {
		ctrl.Render(c, types.NewProvisioningResult(nil, nil, err))
		return
	}
	ctrl.Render(c, types.NewProvisioningResult(&prov, nil, nil))
}

// SetupSSHKeys triggers SSH key setup on a provisioning.
func (ctrl *Controller) SetupSSHKeys(c *gin.Context) {
	principal, _ := controllers.GetPrincipal(c)

	id, err := parseUUID(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	prov, taskID, err := ctrl.service.SetupSSHKeys(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		principal,
		id,
	)
	if err != nil {
		ctrl.Render(c, types.NewProvisioningResult(nil, nil, err))
		return
	}
	ctrl.Render(c, types.NewProvisioningResult(&prov, taskID, nil))
}

// parseUUID parses a UUID string and returns a user-friendly error.
func parseUUID(s string) (uuid.UUID, error) {
	id, err := uuid.FromString(s)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("invalid provisioning ID")
	}
	return id, nil
}
