// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package health

import "net/http"

// HealthDTO is the DTO for the health endpoint.
type HealthDTO struct {
	Data string `json:"data"`
}

// GetData returns the data.
func (dto *HealthDTO) GetData() any {
	return "pong"
}

// GetPublicError always returns nil.
func (dto *HealthDTO) GetError() error {
	return nil
}

func (dto *HealthDTO) GetAssociatedStatusCode() int {
	return http.StatusOK
}
