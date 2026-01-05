// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package controllers

import "github.com/gin-gonic/gin"

// IController defines the main interface for all HTTP controllers in the roachprod-centralized system.
// Controllers handle HTTP requests and coordinate between the transport layer and business logic services.
type IController interface {
	// GetHandlers returns a slice of controller handlers that define the routes and their implementations.
	GetHandlers() []IControllerHandler
	// Authentication performs authentication and authorization for the given gin context.
	// Parameters: context, requireAuth flag, user email, user role, and authentication issuer.
	Authentication(*gin.Context, bool, string, string, string)
}

// IControllerHandler defines the interface for individual HTTP route handlers.
// Each handler represents a specific HTTP endpoint with its method, path, and processing logic.
type IControllerHandler interface {
	// GetHandlers returns the Gin middleware functions that process the HTTP request for this route.
	GetHandlers() []gin.HandlerFunc
	// GetAuthenticationType returns the type of authentication required for this endpoint.
	GetAuthenticationType() AuthenticationType
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
