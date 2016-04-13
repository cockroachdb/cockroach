// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package base

import "time"

const (
	// DefaultPort is the port officially assigned to CockroachDB by the IANA
	// Service Name and Transport Protocol Port Number Registry. See
	// https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml?search=cockroachdb
	DefaultPort = "26257"

	// DefaultHTTPPort is default port for HTTP-for-humans.
	DefaultHTTPPort = "8080"

	// NetworkTimeout is the timeout used for network operations.
	NetworkTimeout = 3 * time.Second

	// DefaultMaxOffset is the standard supported offset between clocks in a
	// CockroachDB cluster. It is currently not possible to increase this
	// value (though a cluster can be run with a smaller offset).
	DefaultMaxOffset = 250 * time.Millisecond
)
