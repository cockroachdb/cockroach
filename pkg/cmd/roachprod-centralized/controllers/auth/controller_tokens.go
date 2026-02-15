// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/auth/types"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/bindings/stripe"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/query"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
)

// ControllerBearerAuth handles authentication-related HTTP requests
// for token-based auth.
type ControllerBearerAuth struct {
	*controllers.Controller
	authService authtypes.IService
	handlers    []controllers.IControllerHandler
}

// NewControllerBearerAuth creates a new tokens authentication controller.
func NewControllerBearerAuth(authService authtypes.IService) *ControllerBearerAuth {
	ctrl := &ControllerBearerAuth{
		Controller:  controllers.NewDefaultController(),
		authService: authService,
	}
	ctrl.handlers = []controllers.IControllerHandler{
		&controllers.ControllerHandler{
			Method:         "POST",
			Path:           types.ControllerPath + "/okta/exchange",
			Func:           ctrl.ExchangeOktaToken,
			Authentication: controllers.AuthenticationTypeNone, // Public endpoint
		},
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath + "/tokens",
			Func:   ctrl.ListSelfTokens,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionTokensViewOwn,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "DELETE",
			Path:   types.ControllerPath + "/tokens/:id",
			Func:   ctrl.RevokeSelfToken,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionTokensRevokeOwn,
				},
			},
		},
	}
	return ctrl
}

// GetControllerHandlers returns the controller's handlers.
func (ctrl *ControllerBearerAuth) GetControllerHandlers() []controllers.IControllerHandler {
	return ctrl.handlers
}

// ExchangeOktaToken exchanges an Okta access token for an opaque bearer token.
// POST /api/v1/auth/okta/exchange
func (ctrl *ControllerBearerAuth) ExchangeOktaToken(c *gin.Context) {
	ctx := c.Request.Context()
	l := ctrl.GetRequestLogger(c)

	var req types.ExchangeOktaTokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		l.Warn("invalid exchange request", "error", err, "path", c.Request.URL.Path)
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: err,
		})
		return
	}

	// Exchange Okta token for opaque token (no principal needed - this is the authentication endpoint)
	token, tokenString, err := ctrl.authService.ExchangeOktaToken(ctx, l, req.OktaIDToken)
	if err != nil {
		l.Warn("okta token exchange failed", "error", err, "path", c.Request.URL.Path)
		ctrl.Render(c, types.NewAuthResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewAuthResult(types.BuildExchangeOktaTokenResponse(token, tokenString), nil))
}

// ListSelfTokens lists all tokens for the current authenticated principal.
// GET /api/v1/auth/tokens
func (ctrl *ControllerBearerAuth) ListSelfTokens(c *gin.Context) {
	ctx := c.Request.Context()
	l := ctrl.GetRequestLogger(c)

	// Extract principal from context (set by bearer authenticator)
	principal, exists := controllers.GetPrincipal(c)
	if !exists {
		ctrl.Render(c, types.NewAuthResult(nil, authtypes.ErrNotAuthenticated))
		return
	}

	var inputDTO types.InputListTokensDTO
	if err := c.ShouldBindWith(&inputDTO, stripe.StripeQuery); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	// Convert to FilterSet and add pagination/sorting
	filterSet := inputDTO.ToFilterSet()
	pagination, sort := query.ParseQueryParams(c, -1)
	filterSet.Pagination = pagination
	filterSet.Sort = sort

	// Validate FilterSet
	if err := filterSet.Validate(); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	tokens, totalCount, err := ctrl.authService.ListSelfTokens(ctx, l, principal, authtypes.InputListTokensDTO{
		Filters: filterSet,
	})

	ctrl.Render(c, types.NewTokensListResult(
		tokens,
		totalCount,
		filterSet.Pagination,
		err,
	))
}

// RevokeSelfToken revokes a specific token owned by the current authenticated principal.
// DELETE /api/v1/auth/tokens/:id
func (ctrl *ControllerBearerAuth) RevokeSelfToken(c *gin.Context) {
	ctx := c.Request.Context()

	// Extract principal from context (set by bearer authenticator)
	principal, exists := controllers.GetPrincipal(c)
	if !exists {
		ctrl.Render(c, types.NewAuthResult(nil, authtypes.ErrNotAuthenticated))
		return
	}

	// Parse token ID from URL
	id, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: errors.New("invalid token ID"),
		})
		return
	}

	// Revoke the token.
	// Ownership is checked inside the service method.
	if err := ctrl.authService.RevokeSelfToken(ctx, ctrl.GetRequestLogger(c), principal, id); err != nil {
		ctrl.Render(c, types.NewAuthResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewAuthResult(types.BuildRevokeTokenResponse(), nil))
}
