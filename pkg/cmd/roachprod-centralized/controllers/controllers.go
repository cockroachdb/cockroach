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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
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
	ErrInvalidJWTIssuer    = fmt.Errorf("invalid JWT issuer")
	ErrInvalidJWTAlgorithm = fmt.Errorf("unsupported JWT algorithm")
	ErrMissingClaims       = fmt.Errorf("missing required JWT claims")
	ErrRateLimitExceeded   = fmt.Errorf("rate limit exceeded")
	ErrRequestTooLarge     = fmt.Errorf("request too large")
	ErrPathTraversal       = fmt.Errorf("path traversal detected")
)

// JWTConfig contains configuration for secure JWT validation
type JWTConfig struct {
	RequiredIssuer     string
	RequiredAudience   string
	AllowedAlgorithms  []string
	ClockSkewTolerance time.Duration
	RequireBothClaims  bool
}

// Controller is a base controller that is embedded in all other controllers.
// It provides a Render method that is used to render the response.
type Controller struct {
	validateToken TokenValidator
	jwtConfig     JWTConfig
	rateLimiter   *rate.Limiter
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
		rateLimiter:   rate.NewLimiter(rate.Limit(10), 20), // 10 req/sec, burst 20
	}
}

// NewControllerWithJWTConfig creates a new controller with enhanced JWT configuration.
func NewControllerWithJWTConfig(validateToken TokenValidator, jwtConfig JWTConfig) *Controller {
	return &Controller{
		validateToken: validateToken,
		jwtConfig:     jwtConfig,
		rateLimiter:   rate.NewLimiter(rate.Limit(10), 20), // 10 req/sec, burst 20
	}
}

// validateTokenSecure performs enhanced JWT validation with security hardening
func (ctrl *Controller) validateTokenSecure(
	ctx context.Context, token string,
) (*idtoken.Payload, error) {
	// 1. Basic validation with audience
	payload, err := ctrl.validateToken(ctx, token, ctrl.jwtConfig.RequiredAudience)
	if err != nil {
		return nil, err
	}

	// 2. Issuer validation
	if ctrl.jwtConfig.RequiredIssuer != "" && payload.Issuer != ctrl.jwtConfig.RequiredIssuer {
		return nil, ErrInvalidJWTIssuer
	}

	// 3. Enhanced claims validation
	if ctrl.jwtConfig.RequireBothClaims {
		sub := payload.Claims["sub"]
		email := payload.Claims["email"]
		if sub == nil || email == nil {
			return nil, ErrMissingClaims
		}
		// Additional validation: ensure claims are strings and not empty
		if subStr, ok := sub.(string); !ok || subStr == "" {
			return nil, ErrMissingClaims
		}
		if emailStr, ok := email.(string); !ok || emailStr == "" {
			return nil, ErrMissingClaims
		}
	}

	return payload, nil
}

// rateLimitCheck checks if the request should be rate limited
func (ctrl *Controller) rateLimitCheck(c *gin.Context) bool {
	if ctrl.rateLimiter == nil {
		return true // Allow if no rate limiter configured
	}
	return ctrl.rateLimiter.Allow()
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

// Authentication is a middleware that checks the Authorization header with enhanced security.
// It takes a gin.Context, an authDisabled flag, an authHeader string, and an
// authAudience string.
// If authDisabled is true, the middleware skips the authentication.
// If authDisabled is false, the middleware validates the token using enhanced validation.
func (ctrl *Controller) Authentication(
	c *gin.Context, authDisabled bool, authHeader, authAudience string,
) {
	l := ctrl.GetRequestLogger(c)

	// Rate limiting check
	if !ctrl.rateLimitCheck(c) {
		l.Warn("Rate limit exceeded", slog.String("client_ip", c.ClientIP()))
		c.JSON(http.StatusTooManyRequests, &ApiResponse{PublicError: ErrRateLimitExceeded.Error()})
		c.Abort()
		return
	}

	if authDisabled {
		l.Debug("Authentication is disabled, skipping")
		c.Next()
		return
	}

	token := c.GetHeader(authHeader)
	if token == "" {
		l.Debug("Missing JWT token in header", slog.String("header", authHeader))
		c.JSON(http.StatusUnauthorized, &ApiResponse{PublicError: ErrUnauthorized.Error()})
		c.Abort()
		return
	}

	// Use enhanced JWT validation if configured, otherwise fallback to basic
	var payload *idtoken.Payload
	var err error

	if ctrl.jwtConfig.RequiredAudience != "" || ctrl.jwtConfig.RequiredIssuer != "" {
		// Set audience from parameter if not configured
		if ctrl.jwtConfig.RequiredAudience == "" {
			ctrl.jwtConfig.RequiredAudience = authAudience
		}
		payload, err = ctrl.validateTokenSecure(c, token)
	} else {
		payload, err = ctrl.validateToken(c, token, authAudience)
	}

	if err != nil {
		l.Warn("JWT validation failed",
			slog.Any("error", err),
			slog.String("client_ip", c.ClientIP()),
			slog.String("user_agent", c.GetHeader("User-Agent")))
		c.JSON(http.StatusUnauthorized, &ApiResponse{PublicError: ErrUnauthorized.Error()})
		c.Abort()
		return
	}

	// Enhanced claims validation
	userID := payload.Claims["sub"]
	userEmail := payload.Claims["email"]

	// Require both claims if enhanced validation is enabled
	if ctrl.jwtConfig.RequireBothClaims && (userID == nil || userEmail == nil) {
		l.Warn("Missing required JWT claims", slog.String("client_ip", c.ClientIP()))
		c.JSON(http.StatusUnauthorized, &ApiResponse{PublicError: ErrUnauthorized.Error()})
		c.Abort()
		return
	}

	// Fallback: require at least one claim for basic validation
	if userID == nil && userEmail == nil {
		l.Warn("JWT is valid, but missing both userID and email claims")
		c.JSON(http.StatusUnauthorized, &ApiResponse{PublicError: ErrUnauthorized.Error()})
		c.Abort()
		return
	}

	// Set the user ID and email in the request context
	c.Set(SessionUserID, userID)
	c.Set(SessionUserEmail, userEmail)

	// Log successful authentication
	l.Debug("JWT authentication successful",
		slog.String("user_id", fmt.Sprintf("%v", userID)),
		slog.String("email", fmt.Sprintf("%v", userEmail)))

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
