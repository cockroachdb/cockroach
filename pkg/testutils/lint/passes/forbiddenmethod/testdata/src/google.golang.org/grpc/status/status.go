// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package status

import (
	istatus "google.golang.org/grpc/internal/status"
)

// This mirrors the gRPC structure, where status.Status is an alias for an
// internal type. The lint needs to check on the internal type.
type Status = istatus.Status

func New(_ int, _ string) *Status {
	return &Status{}
}
