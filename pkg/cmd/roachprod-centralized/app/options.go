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

type WithApiGinEngineOption struct {
	value *gin.Engine
}

func (o WithApiGinEngineOption) apply(a *App) {
	a.api.engine = o.value
}

func WithApiGinEngine(value *gin.Engine) IAppOption {
	return WithApiGinEngineOption{value: value}
}

type WithApiControllerOption struct {
	value controllers.IController
}

func (o WithApiControllerOption) apply(a *App) {
	if a.api.controllers == nil {
		a.api.controllers = make([]controllers.IController, 0)
	}
	a.api.controllers = append(a.api.controllers, o.value)
}

func WithApiController(value controllers.IController) IAppOption {
	return WithApiControllerOption{value: value}
}

type WithApiBaseURLOption struct {
	value string
}

func (o WithApiBaseURLOption) apply(a *App) {
	a.api.baseURL = o.value
}

func WithApiBaseURL(baseURL string) IAppOption {
	return WithApiBaseURLOption{value: baseURL}
}

type WithApiMetricsOption struct {
	value bool
}

func (o WithApiMetricsOption) apply(a *App) {
	a.api.metrics = o.value
}

func WithApiMetrics(metrics bool) IAppOption {
	return WithApiMetricsOption{value: metrics}
}

type WithApiPortOption struct {
	value int
}

func (o WithApiPortOption) apply(a *App) {
	a.api.port = o.value
}

func WithApiPort(port int) IAppOption {
	return WithApiPortOption{value: port}
}

type WithApiMetricsPortOption struct {
	value int
}

func (o WithApiMetricsPortOption) apply(a *App) {
	a.api.metricsPort = o.value
}

func WithApiMetricsPort(port int) IAppOption {
	return WithApiMetricsPortOption{value: port}
}

type WithAppLogLevelOption struct {
	value string
}

func (o WithAppLogLevelOption) apply(a *App) {
	a.logLevel = o.value
}

func WithAppLogLevel(logLevel string) IAppOption {
	return WithAppLogLevelOption{value: logLevel}
}

type WithServiceOption struct {
	value services.IService
}

func (o WithServiceOption) apply(a *App) {
	if a.services == nil {
		a.services = make([]services.IService, 0)
	}
	a.services = append(a.services, o.value)
}

func WithService(value services.IService) IAppOption {
	return WithServiceOption{value: value}
}

type WithApiAuthenticationDisabledOption struct {
	value bool
}

func (o WithApiAuthenticationDisabledOption) apply(a *App) {
	a.api.authenticationDisabled = o.value
}

func WithApiAuthenticationDisabled(enabled bool) IAppOption {
	return WithApiAuthenticationDisabledOption{value: enabled}
}

type WithApiAuthenticationAudienceOption struct {
	value string
}

func (o WithApiAuthenticationAudienceOption) apply(a *App) {
	a.api.authenticationAudience = o.value
}

func WithApiAuthenticationAudience(audience string) IAppOption {
	return WithApiAuthenticationAudienceOption{value: audience}
}

type WithApiAuthenticationHeaderOption struct {
	value string
}

func (o WithApiAuthenticationHeaderOption) apply(a *App) {
	a.api.authenticationHeader = o.value
}

func WithApiAuthenticationHeader(header string) IAppOption {
	return WithApiAuthenticationHeaderOption{value: header}
}
