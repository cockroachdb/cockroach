// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package app

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services"
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

func WithService(value services.IService) OptionFunc {
	return func(a *App) {
		if a.services == nil {
			a.services = make([]services.IService, 0)
		}
		a.services = append(a.services, value)
	}
}

func WithApiAuthenticationDisabled(enabled bool) OptionFunc {
	return func(a *App) {
		a.api.authenticationDisabled = enabled
	}
}

func WithApiAuthenticationAudience(audience string) OptionFunc {
	return func(a *App) {
		a.api.authenticationAudience = audience
	}
}

func WithApiAuthenticationHeader(header string) OptionFunc {
	return func(a *App) {
		a.api.authenticationHeader = header
	}
}
