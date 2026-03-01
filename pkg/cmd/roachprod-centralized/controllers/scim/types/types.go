// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/errors"
)

const (
	// ControllerPath is the base path for SCIM (System for Cross-domain Identity Management) endpoints.
	ControllerPath = "/scim/v2"

	// SCIM 2.0 Schema URNs (RFC 7643)
	// Core resource schemas
	SchemaUser                  = "urn:ietf:params:scim:schemas:core:2.0:User"
	SchemaGroup                 = "urn:ietf:params:scim:schemas:core:2.0:Group"
	SchemaServiceProviderConfig = "urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig"
	SchemaResourceType          = "urn:ietf:params:scim:schemas:core:2.0:ResourceType"
	SchemaSchema                = "urn:ietf:params:scim:schemas:core:2.0:Schema"

	// API message schemas
	SchemaListResponse = "urn:ietf:params:scim:api:messages:2.0:ListResponse"
	SchemaError        = "urn:ietf:params:scim:api:messages:2.0:Error"
	SchemaPatchOp      = "urn:ietf:params:scim:api:messages:2.0:PatchOp"
)

// UserResource represents a User resource for identity management.
type UserResource struct {
	Schemas    []string                 `json:"schemas"`
	ID         string                   `json:"id"`
	ExternalID string                   `json:"externalId,omitempty"`
	UserName   string                   `json:"userName"`
	Name       map[string]string        `json:"name,omitempty"`
	Emails     []map[string]interface{} `json:"emails,omitempty"`
	Active     bool                     `json:"active"`
	Groups     []map[string]string      `json:"groups,omitempty"`
	Meta       map[string]string        `json:"meta,omitempty"`
}

// CreateUserRequest represents a user creation request.
type CreateUserRequest struct {
	Schemas    []string                 `json:"schemas"`
	ExternalID string                   `json:"externalId"`
	UserName   string                   `json:"userName" binding:"required"`
	Name       map[string]string        `json:"name"`
	Emails     []map[string]interface{} `json:"emails"`
	Active     *bool                    `json:"active"`
	Groups     []map[string]string      `json:"groups"`
}

// SCIMResultError represents the standard error for identity management responses
type SCIMResultError struct {
	Error error
}

// GetError returns the error from the SCIMResultError for identity management operations.
func (dto *SCIMResultError) GetError() error {
	return dto.Error
}

// GetData returns nil for error responses in the identity management system.
func (dto *SCIMResultError) GetData() any {
	return nil
}

// GetAssociatedStatusCode returns the HTTP status code based on the request method,
// data presence, and error state per SCIM 2.0 spec (RFC 7644).
// - DELETE with no error: 204 No Content (section 3.6)
// - POST with no error: 201 Created (section 3.3)
// - All other success cases: 200 OK
// - Error cases: appropriate error status codes
func (dto *SCIMResultError) GetAssociatedStatusCode(method string) int {
	err := dto.GetError()
	switch {
	case err == nil:
		// Success cases depend on HTTP method per RFC 7644
		switch method {
		case http.MethodDelete:
			return http.StatusNoContent // 204
		case http.MethodPost:
			return http.StatusCreated // 201
		default:
			return http.StatusOK // 200
		}
	// 404 Not Found - resource does not exist
	case errors.Is(err, authtypes.ErrUserNotFound),
		errors.Is(err, authtypes.ErrGroupNotFound):
		return http.StatusNotFound
	// 400 Bad Request - invalid syntax or schema violation
	case errors.Is(err, authtypes.ErrInvalidUUID),
		errors.Is(err, authtypes.ErrRequiredField),
		errors.Is(err, authtypes.ErrInvalidEmail):
		return http.StatusBadRequest
	// 409 Conflict - uniqueness constraint violation (RFC 7644 section 3.12)
	case errors.Is(err, authtypes.ErrDuplicateUser),
		errors.Is(err, authtypes.ErrDuplicateOktaID),
		errors.Is(err, authtypes.ErrDuplicateGroup):
		return http.StatusConflict
	// 403 Forbidden - authorization failure
	case errors.Is(err, authtypes.ErrUnauthorized):
		return http.StatusForbidden
	default:
		// Fall back to generic error handling
		return controllers.GetGenericStatusCode(err)
	}
}

// SCIMResult is the standard result type for controller responses
type SCIMResult struct {
	SCIMResultError
	Data any `json:"data,omitempty"`
}

// GetData returns the data from the result
func (dto *SCIMResult) GetData() any {
	return dto.Data
}

// FromService converts service results to a standard result
func (dto *SCIMResult) FromService(data any, err error) *SCIMResult {
	dto.Data = data
	dto.Error = err
	return dto
}

// BuildSCIMListResponse creates a SCIM 2.0 compliant list response.
// This follows the SCIM 2.0 specification (RFC 7644) for ListResponse format.
//
// Example usage:
//
//	response := types.BuildSCIMListResponse(
//	    userResources,
//	    totalCount,
//	    startIndex,
//	    []string{"urn:ietf:params:scim:api:messages:2.0:ListResponse"},
//	)
func BuildSCIMListResponse[T any](
	resources []T, totalCount int, startIndex int, schemas []string,
) map[string]interface{} {
	return map[string]interface{}{
		"schemas":      schemas,
		"totalResults": totalCount,
		"startIndex":   startIndex,
		"itemsPerPage": len(resources),
		"Resources":    resources,
	}
}

// GroupResource represents a Group resource for identity management.
type GroupResource struct {
	Schemas     []string          `json:"schemas"`
	ID          string            `json:"id"`
	ExternalID  string            `json:"externalId,omitempty"`
	DisplayName string            `json:"displayName"`
	Members     []GroupMemberRef  `json:"members,omitempty"`
	Meta        map[string]string `json:"meta,omitempty"`
}

// GroupMemberRef represents a member reference in a Group resource.
type GroupMemberRef struct {
	Value   string `json:"value"`             // User ID
	Ref     string `json:"$ref,omitempty"`    // URI reference to the user
	Display string `json:"display,omitempty"` // Display name (optional)
}

// CreateGroupRequest represents a group creation request.
type CreateGroupRequest struct {
	Schemas     []string         `json:"schemas"`
	ExternalID  *string          `json:"externalId"`
	DisplayName string           `json:"displayName" binding:"required"`
	Members     []GroupMemberRef `json:"members"`
}
