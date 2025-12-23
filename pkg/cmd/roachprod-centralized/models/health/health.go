// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package health

import "time"

type Mode int

const (
	APIOnly Mode = iota
	WorkersOnly
	APIWithWorkers
)

// InstanceInfo represents information about a service instance.
type InstanceInfo struct {
	InstanceID    string            `json:"instance_id"`
	Hostname      string            `json:"hostname"`
	Mode          Mode              `json:"mode"`
	StartedAt     time.Time         `json:"started_at"`
	LastHeartbeat time.Time         `json:"last_heartbeat"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}
