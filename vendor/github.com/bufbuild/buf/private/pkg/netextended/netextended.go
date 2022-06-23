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

package netextended

import (
	"errors"
	"fmt"
	"net"
)

const (
	domainNameMinLength = 2
	domainNameMaxLength = 254
	maxSegmentLength    = 63
)

// ValidateHostname verifies the given hostname is a well-formed IP address
// or domain name, optionally including a port, and returns the hostname part.
func ValidateHostname(hostname string) (string, error) {
	if len(hostname) == 0 {
		return "", errors.New("must not be empty")
	}
	if len(hostname) < domainNameMinLength || len(hostname) > domainNameMaxLength {
		return "", fmt.Errorf("must be at least %d and at most %d characters", domainNameMinLength, domainNameMaxLength)
	}

	parsedHost := hostname
	if host, _, err := net.SplitHostPort(hostname); err == nil {
		parsedHost = host
	}
	if net.ParseIP(parsedHost) != nil {
		// hostname is a valid IP address
		return hostname, nil
	}
	if err := isValidDomainName(parsedHost); err != nil {
		return "", fmt.Errorf("must either be a valid IP address or domain name: invalid domain name %q, %w", hostname, err)
	}
	return hostname, nil
}

// isValidDomainName validates a hostname according to the requirements set for
// domain names internally in the Go standard library's net package, see
// golang.org/issue/12421.
//
// Adapted from https://github.com/golang/go/blob/f4e7a6b905ce60448e506a3f6578d01b60602cdd/src/net/dnsclient.go#L73-L128
// See https://github.com/golang/go/blob/f4e7a6b905ce60448e506a3f6578d01b60602cdd/LICENSE for the license.
func isValidDomainName(hostname string) error {
	previous := rune('.')
	nonNumeric := false
	segmentLen := 0
	for _, char := range hostname {
		switch {
		case '0' <= char && char <= '9':
			segmentLen++
		case 'a' <= char && char <= 'z' || 'A' <= char && char <= 'Z' || char == '_':
			nonNumeric = true
			segmentLen++
		case char == '-':
			if previous == '.' {
				return fmt.Errorf("cannot begin a segment after a period (.) with a hyphen (-)")
			}
			nonNumeric = true
			segmentLen++
		case char == '.':
			if previous == '.' {
				return fmt.Errorf("cannot contain two periods (.) in a row")
			}
			if previous == '-' {
				return fmt.Errorf("cannot have a hyphen (-) immediately before a period (.)")
			}
			if segmentLen > maxSegmentLength {
				return fmt.Errorf("cannot have segments greater than %v characters between periods (.)", maxSegmentLength)
			}
			segmentLen = 0
		default:
			return fmt.Errorf("included invalid character %q, must only contain letters, digits, periods (.), hyphens (-), or underscores (_)", char)
		}
		previous = char
	}

	if previous == '-' {
		return fmt.Errorf("cannot have a hyphen (-) as the final character")
	}
	if segmentLen > maxSegmentLength {
		return fmt.Errorf("cannot have segments greater than %v characters between periods (.)", maxSegmentLength)
	}
	if !nonNumeric {
		return errors.New("must have at least one non-numeric character")
	}

	return nil
}
