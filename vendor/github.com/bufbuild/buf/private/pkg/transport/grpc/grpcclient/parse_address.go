// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcclient

import (
	"errors"
	"fmt"
	"net"
	"strings"
)

// parseAddress parses the address according to
// https://github.com/grpc/grpc/blob/master/doc/naming.md,
// applying a default scheme if no scheme is set.
// If the address is an IP address, the default scheme is "passthrough",
// otherwise the default scheme is "dns".
func parseAddress(address string) (string, error) {
	var parsedScheme string
	var parsedAuthority string
	var parsedHost string

	if address == "" {
		return "", errors.New("address is required")
	}

	schemeAndAddress := strings.SplitN(address, "://", 2)
	switch len(schemeAndAddress) {
	case 1:
		parsedHost = address
		// infer dns scheme
		parsedScheme = "dns"
		host := address
		if iParsedHost, _, err := net.SplitHostPort(address); err == nil {
			// Remove port if there is one
			host = iParsedHost
		}
		if net.ParseIP(host) != nil {
			// For valid IPs, use the passthrough scheme
			parsedScheme = "passthrough"
		}
	case 2:
		parsedScheme = strings.TrimSuffix(schemeAndAddress[0], "//")
		switch parsedScheme {
		case "dns", "unix":
		default:
			return "", fmt.Errorf(`unexpected gRPC scheme %q, only "dns" and "unix" are supported`, parsedScheme)
		}
		authorityAndHost := strings.SplitN(schemeAndAddress[1], "/", 2)
		if len(authorityAndHost) != 2 {
			return "", fmt.Errorf("malformed address %q, expected authority and host", address)
		}
		parsedAuthority = authorityAndHost[0]
		parsedHost = authorityAndHost[1]
	}

	return parsedScheme + "://" + parsedAuthority + "/" + parsedHost, nil
}
