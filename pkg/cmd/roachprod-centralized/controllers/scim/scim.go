// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scim

import (
	"encoding/json"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/scim/types"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
)

// Controller handles cross-domain identity management protocol endpoints for user provisioning.
type Controller struct {
	*controllers.Controller
	authService authtypes.IService
	handlers    []controllers.IControllerHandler
}

// NewController creates a new identity management controller.
func NewController(authService authtypes.IService) *Controller {
	ctrl := &Controller{
		Controller:  controllers.NewDefaultController(),
		authService: authService,
	}

	// All SCIM endpoints require the manage user permission
	scimAuthz := &auth.AuthorizationRequirement{
		RequiredPermissions: []string{
			authtypes.PermissionScimManageUser,
		},
	}

	ctrl.handlers = []controllers.IControllerHandler{
		// Schema discovery endpoints
		&controllers.ControllerHandler{
			Method:        "GET",
			Path:          types.ControllerPath + "/ServiceProviderConfig",
			Func:          ctrl.ServiceProviderConfig,
			Authorization: scimAuthz,
		},
		&controllers.ControllerHandler{
			Method:        "GET",
			Path:          types.ControllerPath + "/Schemas",
			Func:          ctrl.Schemas,
			Authorization: scimAuthz,
		},
		&controllers.ControllerHandler{
			Method:        "GET",
			Path:          types.ControllerPath + "/ResourceTypes",
			Func:          ctrl.ResourceTypes,
			Authorization: scimAuthz,
		},
		// User endpoints
		&controllers.ControllerHandler{
			Method:        "GET",
			Path:          types.ControllerPath + "/Users",
			Func:          ctrl.ListUsers,
			Authorization: scimAuthz,
		},
		&controllers.ControllerHandler{
			Method:        "GET",
			Path:          types.ControllerPath + "/Users/:id",
			Func:          ctrl.GetUser,
			Authorization: scimAuthz,
		},
		&controllers.ControllerHandler{
			Method:        "POST",
			Path:          types.ControllerPath + "/Users",
			Func:          ctrl.CreateUser,
			Authorization: scimAuthz,
		},
		&controllers.ControllerHandler{
			Method:        "PUT",
			Path:          types.ControllerPath + "/Users/:id",
			Func:          ctrl.ReplaceUser,
			Authorization: scimAuthz,
		},
		&controllers.ControllerHandler{
			Method:        "PATCH",
			Path:          types.ControllerPath + "/Users/:id",
			Func:          ctrl.PatchUser,
			Authorization: scimAuthz,
		},
		&controllers.ControllerHandler{
			Method:        "DELETE",
			Path:          types.ControllerPath + "/Users/:id",
			Func:          ctrl.DeleteUser,
			Authorization: scimAuthz,
		},
		// Group endpoints
		&controllers.ControllerHandler{
			Method:        "GET",
			Path:          types.ControllerPath + "/Groups",
			Func:          ctrl.ListGroups,
			Authorization: scimAuthz,
		},
		&controllers.ControllerHandler{
			Method:        "GET",
			Path:          types.ControllerPath + "/Groups/:id",
			Func:          ctrl.GetGroup,
			Authorization: scimAuthz,
		},
		&controllers.ControllerHandler{
			Method:        "POST",
			Path:          types.ControllerPath + "/Groups",
			Func:          ctrl.CreateGroup,
			Authorization: scimAuthz,
		},
		&controllers.ControllerHandler{
			Method:        "PUT",
			Path:          types.ControllerPath + "/Groups/:id",
			Func:          ctrl.ReplaceGroup,
			Authorization: scimAuthz,
		},
		&controllers.ControllerHandler{
			Method:        "PATCH",
			Path:          types.ControllerPath + "/Groups/:id",
			Func:          ctrl.PatchGroup,
			Authorization: scimAuthz,
		},
		&controllers.ControllerHandler{
			Method:        "DELETE",
			Path:          types.ControllerPath + "/Groups/:id",
			Func:          ctrl.DeleteGroup,
			Authorization: scimAuthz,
		},
	}
	return ctrl
}

// GetControllerHandlers returns the controller's handlers, as required
// by the controllers.IController interface.
func (ctrl *Controller) GetControllerHandlers() []controllers.IControllerHandler {
	return ctrl.handlers
}

// SCIMContentType is the SCIM 2.0 content type per RFC 7644.
const SCIMContentType = "application/scim+json; charset=utf-8"

// renderIdentityResult is a helper to render identity management results.
// SCIM responses use application/scim+json content type per RFC 7644.
// Responses are at the top level without ApiResponse wrapper.
func (ctrl *Controller) renderIdentityResult(c *gin.Context, data any, err error) {
	// Create result to determine status code
	result := &types.SCIMResult{Data: data}
	result.SCIMResultError.Error = err
	statusCode := result.GetAssociatedStatusCode(c.Request.Method)

	// Handle error case - return SCIM 2.0 error format (RFC 7644 section 3.12)
	if err != nil {

		switch {
		case errors.HasType(err, &utils.PublicError{}):
			// If a public error occurred, log as debug
			ctrl.GetRequestLogger(c).Debug(err.Error())
		default:
			// If an internal error occurred, log the error
			ctrl.GetRequestLogger(c).Error(err.Error())
		}

		ctrl.renderSCIMJSON(c, statusCode, gin.H{
			"schemas": []string{types.SchemaError},
			"status":  statusCode,
			"detail":  err.Error(),
		})
		return
	}

	// Return raw SCIM data at top level
	ctrl.renderSCIMJSON(c, statusCode, data)
}

// renderSCIMJSON renders data as JSON with SCIM content type.
func (ctrl *Controller) renderSCIMJSON(c *gin.Context, statusCode int, data any) {
	var body []byte
	if data != nil {
		var err error
		body, err = json.Marshal(data)
		if err != nil {
			ctrl.GetRequestLogger(c).Error(err.Error())
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}
	}
	c.Data(statusCode, SCIMContentType, body)
}
