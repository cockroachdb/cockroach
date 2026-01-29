// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package controllers

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
)

const (
	XRequestIDKeyHeader      string = "X-Request-ID"
	XCloudTraceContextHeader string = "X-Cloud-Trace-Context"
	TraceLogPrefix           string = "logging.googleapis.com/trace"
	SessionUserEmail         string = "session_user_email"
	SessionUserID            string = "session_user_id"
)

var (
	ErrInternalServerError = fmt.Errorf("internal server error")
	ErrUnauthorized        = fmt.Errorf("unauthorized")
	ErrForbidden           = fmt.Errorf("forbidden")
	ErrInvalidJWTIssuer    = fmt.Errorf("invalid JWT issuer")
	ErrMissingClaims       = fmt.Errorf("missing required JWT claims")
	ErrRequestTooLarge     = fmt.Errorf("request too large")
	ErrPathTraversal       = fmt.Errorf("path traversal detected")
)

// Controller is a base controller that is embedded in all other controllers.
// It provides a Render method that is used to render the response.
type Controller struct{}

// NewDefaultController creates a new default controller.
func NewDefaultController() *Controller {
	return &Controller{}
}

// ValidateInputSecurity performs security validation on input strings
func ValidateInputSecurity(input string, maxLength int) error {
	// Check length
	if len(input) > maxLength {
		return ErrRequestTooLarge
	}

	// Check for path traversal
	if strings.Contains(input, "..") || strings.Contains(input, "/") || strings.Contains(input, "\\") {
		return ErrPathTraversal
	}

	// Check for null bytes
	if strings.Contains(input, "\x00") {
		return ErrPathTraversal
	}

	return nil
}

// Render renders the response.
func (ctrl *Controller) Render(c *gin.Context, dto IResultDTO) {

	resp := &ApiResponse{
		Data:      dto.GetData(),
		RequestID: c.GetString(XRequestIDKeyHeader),
	}

	// Check if DTO implements IPaginatedResult for automatic pagination metadata extraction
	if paginatedDTO, ok := dto.(IPaginatedResult); ok {
		resp.Pagination = paginatedDTO.GetPaginationMetadata()
	}

	// Deduce and fill data type if data is provided
	resp.deduceAndFillDataType()

	// Check if an error occurred while processing the request
	err := dto.GetError()
	if err != nil {

		switch {
		case errors.HasType(err, &utils.PublicError{}):
			// If a public error occurred, return it to the client and log as debug
			resp.PublicError = err.Error()
			ctrl.GetRequestLogger(c).Debug(err.Error())
		default:
			// If an internal error occurred, return Internal Server Error
			// and log the error
			resp.PublicError = ErrInternalServerError.Error()
			ctrl.GetRequestLogger(c).Error(err.Error())
		}

	}

	c.JSON(dto.GetAssociatedStatusCode(), resp)
}

// GetRequestLogger returns a contextualized logger for the given request.
func (ctrl *Controller) GetRequestLogger(c *gin.Context) *logger.Logger {

	// The Gin Engine is configured to use the slogFormatter middleware.
	// This middleware  always sets a Logger in the context, so we can safely
	// assume that the logger is always present.
	ginCtxLogger, exists := c.Get("logger")
	if exists {
		logger, _ := ginCtxLogger.(*logger.Logger)
		return logger
	}

	// Just in case the logger is not present in the context,
	// return one to avoid panics
	return logger.DefaultLogger
}

// ControllerHandler is a struct that holds the information needed to register
// a controller handler.
type ControllerHandler struct {
	Method         string
	Path           string
	Func           gin.HandlerFunc
	Extra          []gin.HandlerFunc
	Authentication AuthenticationType
	Authorization  *auth.AuthorizationRequirement
}

// GetRouteHandlers returns the controller route handlers.
func (c *ControllerHandler) GetRouteHandlers() []gin.HandlerFunc {
	return append([]gin.HandlerFunc{c.Func}, c.Extra...)
}

// GetAuthentication returns whether the controller handler requires authentication.
func (c *ControllerHandler) GetAuthenticationType() AuthenticationType {
	return c.Authentication
}

// GetAuthorizationRequirement returns the authorization requirements for this endpoint.
func (c *ControllerHandler) GetAuthorizationRequirement() *auth.AuthorizationRequirement {
	return c.Authorization
}

// GetMethod returns the controller handler's method.
func (c *ControllerHandler) GetMethod() string {
	return c.Method
}

// GetPath returns the controller handler's path.
func (c *ControllerHandler) GetPath() string {
	return c.Path
}

// PaginationMetadata contains pagination information for list responses.
// Embed this struct in result DTOs that return lists to automatically provide pagination metadata.
type PaginationMetadata struct {
	TotalCount int `json:"total_count"`
	Count      int `json:"count"`
	StartIndex int `json:"start_index"`
}

// GetPaginationMetadata returns a pointer to itself.
// This allows any struct embedding PaginationMetadata to automatically implement IPaginatedResult.
func (p *PaginationMetadata) GetPaginationMetadata() *PaginationMetadata {
	return p
}

// ApiResponse is the response object that is sent back to the client.
type ApiResponse struct {
	RequestID   string              `json:"request_id,omitempty"`
	Data        any                 `json:"data,omitempty"`
	Pagination  *PaginationMetadata `json:"pagination,omitempty"`
	ResultType  string              `json:"result_type,omitempty"`
	PublicError string              `json:"error,omitempty"`
}

// deduceAndFillDataType deduces the data type and fills the ResultType field.
func (r *ApiResponse) deduceAndFillDataType() {
	if r.Data == nil {
		return
	}
	if r.ResultType == "" {
		r.ResultType = strings.ReplaceAll(fmt.Sprintf("%T", r.Data), "*", "")
	}
}
