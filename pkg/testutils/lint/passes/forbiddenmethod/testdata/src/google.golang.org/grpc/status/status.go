// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
