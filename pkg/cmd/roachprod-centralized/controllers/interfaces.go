// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package controllers

import "github.com/gin-gonic/gin"

type IController interface {
	GetHandlers() []IControllerHandler
	Authentication(*gin.Context, bool, string, string)
}

type IControllerHandler interface {
	GetHandlers() []gin.HandlerFunc
	GetAuthenticationType() AuthenticationType
	GetMethod() string
	GetPath() string
}

type IResultDTO interface {
	GetData() any
	GetError() error
	GetAssociatedStatusCode() int
}
