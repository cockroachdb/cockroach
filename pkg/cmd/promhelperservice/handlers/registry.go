// Copyright 2024 The Cockroach Authors.

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

	"github.com/cockroachdb/cockroach/pkg/cmd/promhelperservice/handlers/instanceconfigs"
	"github.com/cockroachdb/cockroach/pkg/cmd/promhelperservice/registryhandler"
)

type HandlerRegistry struct{}

// GetRegistrations returns all the URLRegistry.
// Any new URL and a method has to be registered in this function
func (hr *HandlerRegistry) GetRegistrations(_ context.Context) []*registryhandler.URLRegistry {
	return []*registryhandler.URLRegistry{
		{
			Url:        instanceconfigs.FormInsConfUrlWithClusterID(),
			Method:     registryhandler.PUT,
			HandleFunc: instanceconfigs.PutHandler,
		},
		{
			Url:        instanceconfigs.FormInsConfUrlWithClusterID(),
			Method:     registryhandler.DELETE,
			HandleFunc: instanceconfigs.DeleteHandler,
		},
	}
}
