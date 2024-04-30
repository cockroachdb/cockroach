// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Registry of all the URLs, Methods and the corresponding handler functions

package handlers

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/promhelperservice/handlers/healthchecks"
	"github.com/cockroachdb/cockroach/pkg/cmd/promhelperservice/handlers/instanceconfigs"
	"github.com/cockroachdb/cockroach/pkg/cmd/promhelperservice/registryhandler"
)

const (
	urlPrefix = "/promhelpers"
)

type HandlerRegistry struct{}

// RegisterFlags for the handlers
func (hr *HandlerRegistry) RegisterFlags() {
	instanceconfigs.RegisterFlag()
}

// GetRegistrations returns all the URLRegistry.
// Any new URL and a method has to be registered in this function
func (hr *HandlerRegistry) GetRegistrations(_ context.Context) []*registryhandler.URLRegistry {
	return []*registryhandler.URLRegistry{
		{
			Url:        urlPrefix + "/health-checks",
			Method:     registryhandler.GET,
			HandleFunc: healthchecks.HealthCheckHandler,
		},
		{
			Url:        urlPrefix + instanceconfigs.BuildInstanceConfigUrl(),
			Method:     registryhandler.PUT,
			HandleFunc: instanceconfigs.PutHandler,
		},
		{
			Url:        urlPrefix + instanceconfigs.BuildInstanceConfigUrl(),
			Method:     registryhandler.DELETE,
			HandleFunc: instanceconfigs.DeleteHandler,
		},
	}
}
