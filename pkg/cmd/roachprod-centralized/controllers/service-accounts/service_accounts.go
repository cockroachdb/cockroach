// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serviceaccounts

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/service-accounts/types"
	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/bindings/stripe"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/query"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
)

// Controller handles service account-related HTTP requests.
type Controller struct {
	*controllers.Controller
	authService authtypes.IService
	handlers    []controllers.IControllerHandler
}

// NewController creates a new service account controller.
func NewController(authService authtypes.IService) *Controller {
	ctrl := &Controller{
		Controller:  controllers.NewDefaultController(),
		authService: authService,
	}

	ctrl.handlers = []controllers.IControllerHandler{
		&controllers.ControllerHandler{
			Method: "POST",
			Path:   types.ControllerPath,
			Func:   ctrl.Create,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionServiceAccountCreate,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath,
			Func:   ctrl.List,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					authtypes.PermissionServiceAccountViewAll,
					authtypes.PermissionServiceAccountViewOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath + "/:id",
			Func:   ctrl.Get,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					authtypes.PermissionServiceAccountViewAll,
					authtypes.PermissionServiceAccountViewOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "PUT",
			Path:   types.ControllerPath + "/:id",
			Func:   ctrl.Update,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					authtypes.PermissionServiceAccountUpdateAll,
					authtypes.PermissionServiceAccountUpdateOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "DELETE",
			Path:   types.ControllerPath + "/:id",
			Func:   ctrl.Delete,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					authtypes.PermissionServiceAccountDeleteAll,
					authtypes.PermissionServiceAccountDeleteOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "POST",
			Path:   types.ControllerPath + "/:id/tokens",
			Func:   ctrl.MintToken,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					authtypes.PermissionServiceAccountMintAll,
					authtypes.PermissionServiceAccountMintOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "DELETE",
			Path:   types.ControllerPath + "/:id/tokens/:tokenId",
			Func:   ctrl.RevokeToken,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					authtypes.PermissionServiceAccountUpdateAll,
					authtypes.PermissionServiceAccountUpdateOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath + "/:id/tokens",
			Func:   ctrl.ListTokens,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					authtypes.PermissionServiceAccountViewAll,
					authtypes.PermissionServiceAccountViewOwn,
				},
			},
		},
		// Origins endpoints
		&controllers.ControllerHandler{
			Method: "POST",
			Path:   types.ControllerPath + "/:id/origins",
			Func:   ctrl.AddOrigin,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					authtypes.PermissionServiceAccountUpdateAll,
					authtypes.PermissionServiceAccountUpdateOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath + "/:id/origins",
			Func:   ctrl.ListOrigins,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					authtypes.PermissionServiceAccountViewAll,
					authtypes.PermissionServiceAccountViewOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "DELETE",
			Path:   types.ControllerPath + "/:id/origins/:originId",
			Func:   ctrl.RemoveOrigin,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					authtypes.PermissionServiceAccountUpdateAll,
					authtypes.PermissionServiceAccountUpdateOwn,
				},
			},
		},
		// Permissions endpoints
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath + "/:id/permissions",
			Func:   ctrl.ListPermissions,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					authtypes.PermissionServiceAccountViewAll,
					authtypes.PermissionServiceAccountViewOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "POST",
			Path:   types.ControllerPath + "/:id/permissions",
			Func:   ctrl.AddPermission,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					authtypes.PermissionServiceAccountUpdateAll,
					authtypes.PermissionServiceAccountUpdateOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "DELETE",
			Path:   types.ControllerPath + "/:id/permissions/:permissionId",
			Func:   ctrl.RemovePermission,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					authtypes.PermissionServiceAccountUpdateAll,
					authtypes.PermissionServiceAccountUpdateOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "PUT",
			Path:   types.ControllerPath + "/:id/permissions",
			Func:   ctrl.ReplacePermissions,
			Authorization: &auth.AuthorizationRequirement{
				AnyOf: []string{
					authtypes.PermissionServiceAccountUpdateAll,
					authtypes.PermissionServiceAccountUpdateOwn,
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

// Create creates a new service account.
// POST /api/v1/service-accounts
func (ctrl *Controller) Create(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	var req types.CreateServiceAccountRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: err,
		})
		return
	}

	// Create service account
	sa := &authmodels.ServiceAccount{
		ID:          uuid.MakeV4(),
		Name:        req.Name,
		Description: req.Description,
		Enabled:     true,
	}

	if err := ctrl.authService.CreateServiceAccount(ctx, ctrl.GetRequestLogger(c), principal, sa, req.Orphan); err != nil {
		ctrl.Render(c, types.NewServiceAccountsResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewServiceAccountsResult(types.BuildServiceAccountResponse(sa), nil))
}

// List returns all service accounts.
// GET /api/v1/service-accounts
func (ctrl *Controller) List(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	var inputDTO types.InputListDTO
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

	serviceAccounts, totalCount, err := ctrl.authService.ListServiceAccounts(
		ctx, ctrl.GetRequestLogger(c), principal,
		authtypes.InputListServiceAccountsDTO{Filters: filterSet},
	)

	ctrl.Render(c, types.NewServiceAccountsListResult(
		serviceAccounts,
		totalCount,
		filterSet.Pagination,
		err,
	))
}

// Get returns a specific service account by ID.
// GET /api/v1/service-accounts/:id
func (ctrl *Controller) Get(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	// Parse service account ID from URL
	id, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: errors.New("invalid service account ID")})
		return
	}

	sa, err := ctrl.authService.GetServiceAccount(ctx, ctrl.GetRequestLogger(c), principal, id)
	if err != nil {
		if errors.Is(err, authtypes.ErrServiceAccountNotFound) {
			ctrl.Render(c, types.NewServiceAccountsResult(nil, authtypes.ErrServiceAccountNotFound))
			return
		}
		ctrl.Render(c, types.NewServiceAccountsResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewServiceAccountsResult(types.BuildServiceAccountResponse(sa), nil))
}

// Update updates a service account.
// PUT /api/v1/service-accounts/:id
func (ctrl *Controller) Update(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	// Parse service account ID from URL
	id, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: errors.New("invalid service account ID")})
		return
	}

	var req types.UpdateServiceAccountRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: err,
		})
		return
	}

	// Update service account (field updates handled by service layer)
	sa, err := ctrl.authService.UpdateServiceAccount(
		ctx, ctrl.GetRequestLogger(c), principal,
		id, req.ToServiceDTO(),
	)
	if err != nil {
		if errors.Is(err, authtypes.ErrServiceAccountNotFound) {
			ctrl.Render(c, types.NewServiceAccountsResult(nil, authtypes.ErrServiceAccountNotFound))
			return
		}
		ctrl.Render(c, types.NewServiceAccountsResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewServiceAccountsResult(types.BuildServiceAccountResponse(sa), nil))
}

// Delete deletes a service account.
// DELETE /api/v1/service-accounts/:id
func (ctrl *Controller) Delete(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	// Parse service account ID from URL
	id, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: errors.New("invalid service account ID")})
		return
	}

	if err := ctrl.authService.DeleteServiceAccount(ctx, ctrl.GetRequestLogger(c), principal, id); err != nil {
		ctrl.Render(c, types.NewServiceAccountsResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewServiceAccountsResult(types.BuildDeleteResponse(), nil))
}

// MintToken creates a new API token for the service account.
// POST /api/v1/service-accounts/:id/tokens
func (ctrl *Controller) MintToken(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	// Parse service account ID from URL
	id, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: errors.New("invalid service account ID")})
		return
	}

	var req types.MintTokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: err,
		})
		return
	}

	// Mint token
	ttl := authtypes.TokenDefaultTTLServiceAccount
	if req.TTLDays > 0 {
		ttl = time.Duration(req.TTLDays) * 24 * time.Hour
	}
	token, tokenString, err := ctrl.authService.MintServiceAccountToken(ctx, ctrl.GetRequestLogger(c), principal, id, ttl)
	if err != nil {
		ctrl.Render(c, types.NewServiceAccountsResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewServiceAccountsResult(types.BuildMintTokenResponse(token.ID, tokenString, &token.ExpiresAt), nil))
}

// RevokeToken revokes a specific service account token.
// DELETE /api/v1/service-accounts/:id/tokens/:tokenId
func (ctrl *Controller) RevokeToken(c *gin.Context) {
	ctx := c.Request.Context()

	// Parse service account ID from URL
	saID, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: errors.New("invalid service account ID")})
		return
	}

	// Parse token ID from URL
	tokenIDStr := c.Param("tokenId")
	tokenID, err := uuid.FromString(tokenIDStr)
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: errors.New("invalid token ID")})
		return
	}

	principal, _ := controllers.GetPrincipal(c)

	// Revoke the token for this service account.
	// Service verifies token belongs to this service account.
	if err := ctrl.authService.RevokeServiceAccountToken(ctx, ctrl.GetRequestLogger(c), principal, saID, tokenID); err != nil {
		ctrl.Render(c, types.NewServiceAccountsResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewServiceAccountsResult(types.BuildRevokeTokenResponse(), nil))
}

// ListTokens lists all tokens for a service account.
// GET /api/v1/service-accounts/:id/tokens
func (ctrl *Controller) ListTokens(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	// Parse service account ID from URL
	saID, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: errors.New("invalid service account ID")})
		return
	}

	// Parse filter parameters
	pagination, sort := query.ParseQueryParams(c, -1) // default: unlimited
	filterSet := authtypes.NewInputListServiceAccountTokensDTO().Filters
	filterSet.Pagination = pagination
	filterSet.Sort = sort

	// Validate FilterSet
	if err := filterSet.Validate(); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	tokens, totalCount, err := ctrl.authService.ListServiceAccountTokens(
		ctx, ctrl.GetRequestLogger(c), principal, saID,
		authtypes.InputListServiceAccountTokensDTO{Filters: filterSet},
	)
	if err != nil {
		ctrl.Render(c, types.NewServiceAccountsResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewTokensListResult(tokens, totalCount, filterSet.Pagination, nil))
}

// AddOrigin adds an IP origin restriction to a service account.
// POST /api/v1/service-accounts/:id/origins
func (ctrl *Controller) AddOrigin(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	// Parse service account ID from URL
	saIDStr := c.Param("id")
	saID, err := uuid.FromString(saIDStr)
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: errors.New("invalid service account ID")})
		return
	}

	var req types.AddOriginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	// Create origin model (service will generate UUID and set timestamps)
	origin := &authmodels.ServiceAccountOrigin{
		ServiceAccountID: saID,
		CIDR:             req.CIDR,
		Description:      req.Description,
	}

	if err := ctrl.authService.AddServiceAccountOrigin(ctx, ctrl.GetRequestLogger(c), principal, saID, origin); err != nil {
		ctrl.Render(c, types.NewServiceAccountsResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewServiceAccountsResult(types.BuildOriginResponse(origin), nil))
}

// ListOrigins lists all IP origin restrictions for a service account.
// GET /api/v1/service-accounts/:id/origins
func (ctrl *Controller) ListOrigins(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	// Parse service account ID from URL
	saID, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: errors.New("invalid service account ID")})
		return
	}

	var inputDTO types.InputListOriginsDTO
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

	origins, totalCount, err := ctrl.authService.ListServiceAccountOrigins(ctx, ctrl.GetRequestLogger(c), principal, saID, authtypes.InputListServiceAccountOriginsDTO{Filters: filterSet})
	if err != nil {
		ctrl.Render(c, types.NewServiceAccountsResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewOriginsListResult(
		origins,
		totalCount,
		filterSet.Pagination,
		err,
	))
}

// RemoveOrigin removes an IP origin restriction from a service account.
// DELETE /api/v1/service-accounts/:id/origins/:originId
func (ctrl *Controller) RemoveOrigin(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	// Parse service account ID from URL
	saID, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: errors.New("invalid service account ID")})
		return
	}

	// Parse origin ID from URL
	originIDStr := c.Param("originId")
	originID, err := uuid.FromString(originIDStr)
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: errors.New("invalid origin ID")})
		return
	}

	if err := ctrl.authService.RemoveServiceAccountOrigin(ctx, ctrl.GetRequestLogger(c), principal, saID, originID); err != nil {
		ctrl.Render(c, types.NewServiceAccountsResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewServiceAccountsResult(types.BuildRemoveOriginResponse(), nil))
}

// ListPermissions lists all permissions for a service account.
// GET /api/v1/service-accounts/:id/permissions
func (ctrl *Controller) ListPermissions(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	// Parse service account ID from URL
	saIDStr := c.Param("id")
	saID, err := uuid.FromString(saIDStr)
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: errors.New("invalid service account ID")})
		return
	}

	// Parse filter parameters
	var inputDTO types.InputListPermissionsDTO
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

	// Call service
	permissions, totalCount, err := ctrl.authService.ListServiceAccountPermissions(
		ctx,
		ctrl.GetRequestLogger(c),
		principal,
		saID,
		authtypes.InputListServiceAccountPermissionsDTO{Filters: filterSet},
	)

	if err != nil {
		ctrl.Render(c, types.NewPermissionsListResult(nil, 0, nil, err))
		return
	}

	ctrl.Render(c, types.NewPermissionsListResult(permissions, totalCount, filterSet.Pagination, nil))
}

// AddPermission adds a single permission to a service account.
// POST /api/v1/service-accounts/:id/permissions
func (ctrl *Controller) AddPermission(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	// Parse service account ID from URL
	saID, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: errors.New("invalid service account ID")})
		return
	}

	var req types.AddPermissionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	// Create permission model (service will generate UUID and set timestamps)
	permission := &authmodels.ServiceAccountPermission{
		ServiceAccountID: saID,
		Scope:            req.Scope,
		Permission:       req.Permission,
	}

	if err := ctrl.authService.AddServiceAccountPermission(ctx, ctrl.GetRequestLogger(c), principal, saID, permission); err != nil {
		ctrl.Render(c, types.NewServiceAccountsResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewServiceAccountsResult(types.BuildPermissionResponse(permission), nil))
}

// RemovePermission removes a single permission from a service account.
// DELETE /api/v1/service-accounts/:id/permissions/:permissionId
func (ctrl *Controller) RemovePermission(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	// Parse service account ID from URL
	saID, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: errors.New("invalid service account ID")})
		return
	}

	// Parse permission ID from URL
	permissionIDStr := c.Param("permissionId")
	permissionID, err := uuid.FromString(permissionIDStr)
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: errors.New("invalid permission ID")})
		return
	}

	if err := ctrl.authService.RemoveServiceAccountPermission(ctx, ctrl.GetRequestLogger(c), principal, saID, permissionID); err != nil {
		ctrl.Render(c, types.NewServiceAccountsResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewServiceAccountsResult(types.BuildRemovePermissionResponse(), nil))
}

// ReplacePermissions replaces all permissions for a service account.
// PUT /api/v1/service-accounts/:id/permissions
func (ctrl *Controller) ReplacePermissions(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	// Parse service account ID from URL
	saID, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: errors.New("invalid service account ID")})
		return
	}

	var req types.ReplacePermissionsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	// Convert request DTOs to models
	permissions := make([]*authmodels.ServiceAccountPermission, len(req.Permissions))
	for i, p := range req.Permissions {
		permissions[i] = &authmodels.ServiceAccountPermission{
			ServiceAccountID: saID,
			Scope:            p.Scope,
			Permission:       p.Permission,
		}
	}

	if err := ctrl.authService.UpdateServiceAccountPermissions(ctx, ctrl.GetRequestLogger(c), principal, saID, permissions); err != nil {
		ctrl.Render(c, types.NewServiceAccountsResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewServiceAccountsResult(types.BuildReplacePermissionsResponse(), nil))
}
