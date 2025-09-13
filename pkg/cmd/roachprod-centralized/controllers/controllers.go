// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package controllers

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"google.golang.org/api/idtoken"
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
)

// Controller is a base controller that is embedded in all other controllers.
// It provides a Render method that is used to render the response.
type Controller struct {
	validateToken TokenValidator
}

// TokenValidator is a function that validates a token.
type TokenValidator func(ctx context.Context, token, audience string) (*idtoken.Payload, error)

// NewDefaultController creates a new default controller
// with the default token validator.
func NewDefaultController() *Controller {
	return &Controller{
		validateToken: idtoken.Validate,
	}
}

// NewControllerWithTokenValidator creates a new controller with the given
// token validator.
func NewControllerWithTokenValidator(validateToken TokenValidator) *Controller {
	return &Controller{
		validateToken: validateToken,
	}
}

// Authentication is a middleware that checks the Authorization header.
// It takes a gin.Context, an authDisabled flag, an authHeader string, and an
// authAudience string.
// If authDisabled is true, the middleware skips the authentication.
// If authDisabled is false, the middleware validates the token using the
// controller's token validator.
func (ctrl *Controller) Authentication(
	c *gin.Context, authDisabled bool, authHeader, authAudience string,
) {
	l := ctrl.GetRequestLogger(c)

	if authDisabled {
		l.Debug("Authentication is disabled, skipping")
		c.Next()
		return
	}

	payload, err := ctrl.validateToken(c, c.GetHeader(authHeader), authAudience)
	if err != nil {
		l.Debug("JWT is invalid; returning unauthorized", slog.Any("error", err))
		c.JSON(http.StatusUnauthorized, &ApiResponse{PublicError: ErrUnauthorized.Error()})
		c.Abort()
		return
	}

	// Store user ID and email in the context
	userID := payload.Claims["sub"]
	userEmail := payload.Claims["email"]
	if userID == nil && userEmail == nil {
		l.Warn("JWT is valid, but userID nor email values; returning unauthorized")
		c.JSON(http.StatusUnauthorized, &ApiResponse{PublicError: ErrUnauthorized.Error()})
		c.Abort()
		return
	}

	// Set the user ID and email in the request context
	c.Set(SessionUserID, userID)
	c.Set(SessionUserEmail, userEmail)

	c.Next()
}

// Render renders the response.
func (ctrl *Controller) Render(c *gin.Context, dto IResultDTO) {

	resp := &ApiResponse{
		Data:      dto.GetData(),
		RequestID: c.GetString(XRequestIDKeyHeader),
	}

	// Deduce and fill data type if data is provided
	resp.deduceAndFillDataType()

	// Check if an error occurred while processing the request
	err := dto.GetError()
	if err != nil {

		switch {
		case errors.HasType(err, &utils.PublicError{}):
			// If a public error occurred, return it to the client
			resp.PublicError = err.Error()
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
func (ctrl *Controller) GetRequestLogger(c *gin.Context) *utils.Logger {

	// The Gin Engine is configured to use the slogFormatter middleware.
	// This middleware  always sets a Logger in the context, so we can safely
	// assume that the logger is always present.
	ginCtxLogger, exists := c.Get("logger")
	if exists {
		logger, _ := ginCtxLogger.(*utils.Logger)
		return logger
	}

	// Just in case the logger is not present in the context,
	// return one to avoid panics
	return utils.DefaultLogger
}

// ControllerHandler is a struct that holds the information needed to register
// a controller handler.
type ControllerHandler struct {
	Method         string
	Path           string
	Func           gin.HandlerFunc
	Extra          []gin.HandlerFunc
	Authentication AuthenticationType
}

// GetHandlers returns the controller handler's handlers.
func (c *ControllerHandler) GetHandlers() []gin.HandlerFunc {
	return append([]gin.HandlerFunc{c.Func}, c.Extra...)
}

// GetAuthentication returns whether the controller handler requires authentication.
func (c *ControllerHandler) GetAuthenticationType() AuthenticationType {
	return c.Authentication
}

// GetMethod returns the controller handler's method.
func (c *ControllerHandler) GetMethod() string {
	return c.Method
}

// GetPath returns the controller handler's path.
func (c *ControllerHandler) GetPath() string {
	return c.Path
}

// ApiResponse is the response object that is sent back to the client.
type ApiResponse struct {
	RequestID   string `json:"request_id,omitempty"`
	Data        any    `json:"data,omitempty"`
	ResultType  string `json:"result_type,omitempty"`
	PublicError string `json:"error,omitempty"`
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
