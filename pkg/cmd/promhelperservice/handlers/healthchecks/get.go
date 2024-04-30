// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package healthchecks

import "net/http"

// HealthCheckHandler handles GET /healthcheck
// This is needed for health check of backend service
func HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	_, _ = w.Write([]byte("The service is healthy.\n"))
	w.WriteHeader(http.StatusOK)
}
