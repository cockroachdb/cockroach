// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package admitter

import "time"

// Service provides the interface for performing admission checks before
// allowing requests into sqlproxy.
type Service interface {
	// AllowRequest determines whether a request should be allowed to proceed. It
	// rate limits requests from IP addresses regardless of tenant id.
	AllowRequest(ipAddress string, now time.Time) error
}
