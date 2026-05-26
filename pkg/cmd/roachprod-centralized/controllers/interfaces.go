// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package controllers

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/gin-gonic/gin"
)

// IController defines the main interface for all HTTP controllers in the roachprod-centralized system.
// Controllers handle HTTP requests and coordinate between the transport layer and business logic services.
type IController interface {
	// GetControllerHandlers returns a slice of controller handlers that define the routes and their implementations.
	GetControllerHandlers() []IControllerHandler

	// AuthMiddleware creates authentication middleware.
	// Used by the API layer to add authentication to routes.
	AuthMiddleware(authenticator auth.IAuthenticator, headerName string) gin.HandlerFunc

	// AuthzMiddleware creates authorization middleware.
	// Used by the API layer to add authorization checks to routes.
	AuthzMiddleware(authenticator auth.IAuthenticator, requirement *auth.AuthorizationRequirement) gin.HandlerFunc
}

// IControllerHandler defines the interface for individual HTTP route handlers.
// Each handler represents a specific HTTP endpoint with its method, path, and processing logic.
type IControllerHandler interface {
	// GetRouteHandlers returns the Gin middleware functions that process the HTTP request for this route.
	GetRouteHandlers() []gin.HandlerFunc
	// GetAuthenticationType returns the type of authentication required for this endpoint.
	GetAuthenticationType() AuthenticationType
	// GetAuthorizationRequirement returns the authorization requirements for this endpoint.
	// Returns nil if no authorization is required beyond authentication.
	GetAuthorizationRequirement() *auth.AuthorizationRequirement
	// GetMethod returns the HTTP method (GET, POST, PUT, DELETE, etc.) for this route.
	GetMethod() string
	// GetPath returns the URL path pattern for this route (e.g., "/api/clusters").
	GetPath() string
}

// IResultDTO defines the interface for data transfer objects that encapsulate API response data.
// Result DTOs provide a consistent structure for HTTP responses across all endpoints.
type IResultDTO interface {
	// GetData returns the payload data to be included in the HTTP response body.
	GetData() any
	// GetError returns any error that occurred during request processing.
	GetError() error
	// GetAssociatedStatusCode returns the HTTP status code that should be sent with this response.
	GetAssociatedStatusCode() int
}

// IPaginatedResult extends IResultDTO for list responses that include pagination metadata.
// When a result DTO implements this interface, the Render method will automatically
// extract and populate the pagination field in the API response.
type IPaginatedResult interface {
	IResultDTO
	// GetPaginationMetadata returns the pagination information for list responses.
	GetPaginationMetadata() *PaginationMetadata
}
