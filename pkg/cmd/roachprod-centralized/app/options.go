// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package app

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/gin-gonic/gin"
)

type IAppOption interface {
	apply(app *App)
}

type OptionFunc func(a *App)

func (o OptionFunc) apply(a *App) {
	o(a)
}

func WithApiGinEngine(value *gin.Engine) OptionFunc {
	return func(a *App) {
		a.api.engine = value
	}
}

func WithApiController(value controllers.IController) OptionFunc {
	return func(a *App) {
		if a.api.controllers == nil {
			a.api.controllers = make([]controllers.IController, 0)
		}
		a.api.controllers = append(a.api.controllers, value)
	}
}

func WithApiBaseURL(baseURL string) OptionFunc {
	return func(a *App) {
		a.api.baseURL = baseURL
	}
}

func WithApiMetrics(metrics bool) OptionFunc {
	return func(a *App) {
		a.api.metrics = metrics
	}
}

func WithApiPort(port int) OptionFunc {
	return func(a *App) {
		a.api.port = port
	}
}

func WithApiMetricsPort(port int) OptionFunc {
	return func(a *App) {
		a.api.metricsPort = port
	}
}

func WithAppLogLevel(logLevel string) OptionFunc {
	return func(a *App) {
		a.logLevel = logLevel
	}
}

func WithLogger(logger *logger.Logger) OptionFunc {
	return func(a *App) {
		a.logger = logger
	}
}

func WithService(value services.IService) OptionFunc {
	return func(a *App) {
		if a.services == nil {
			a.services = make([]services.IService, 0)
		}
		a.services = append(a.services, value)
	}
}

func WithServices(values []services.IService) OptionFunc {
	return func(a *App) {
		if a.services == nil {
			a.services = make([]services.IService, 0)
		}
		a.services = append(a.services, values...)
	}
}

// WithApiTrustedProxies sets the trusted proxy CIDRs for X-Forwarded-For parsing.
// When nil or empty, Gin uses RemoteAddr directly (no proxy headers trusted).
func WithApiTrustedProxies(proxies []string) OptionFunc {
	return func(a *App) {
		a.api.trustedProxies = proxies
	}
}

// WithApiAuthenticationHeader sets the HTTP header to extract authentication tokens from.
func WithApiAuthenticationHeader(header string) OptionFunc {
	return func(a *App) {
		a.api.authHeader = header
	}
}

// WithApiAuthenticator sets the authentication implementation.
// The authenticator type determines behavior (disabled, jwt, bearer).
func WithApiAuthenticator(authenticator auth.IAuthenticator) OptionFunc {
	return func(a *App) {
		a.api.authenticator = authenticator
	}
}
