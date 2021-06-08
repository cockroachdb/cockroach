// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgurl

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// Validate checks that the URL parameters are correct.
func (u *URL) Validate() error {
	var details bytes.Buffer
	var incorrect []redact.RedactableString

	switch u.net {
	case ProtoUnix:
		if !strings.HasPrefix(u.host, "/") {
			incorrect = append(incorrect, "host")
			fmt.Fprintln(&details, "Host parameter must start with '/' when using unix sockets.")
		}
		if u.sec != tnUnspecified && u.sec != tnNone {
			incorrect = append(incorrect, "sslmode")
			fmt.Fprintln(&details, "Cannot specify TLS settings when using unix sockets.")
		}
	case ProtoTCP:
		if strings.Contains(u.host, "/") {
			incorrect = append(incorrect, "host")
			fmt.Fprintln(&details, "Host parameter cannot contain '/' when using TCP.")
		}
	default:
		incorrect = append(incorrect, "net")
		fmt.Fprintln(&details, "Network protocol unspecified.")
	}

	if u.username == "" && u.hasPassword {
		incorrect = append(incorrect, "user")
		fmt.Fprintln(&details, "Username cannot be empty when a password is provided.")
	}

	switch u.authn {
	case authnClientCert, authnPasswordWithClientCert:
		if u.sec == tnUnspecified || u.sec == tnNone {
			incorrect = append(incorrect, "sslmode")
			fmt.Fprintln(&details, "Cannot use TLS client certificate authentication without a TLS transport.")
		}
		if u.clientCertPath == "" {
			incorrect = append(incorrect, "sslcert")
			fmt.Fprintln(&details, "Client certificate missing.")
		}
		if u.clientKeyPath == "" {
			incorrect = append(incorrect, "sslkey")
			fmt.Fprintln(&details, "Client key missing.")
		}
	case authnUndefined:
		incorrect = append(incorrect, "authn")
		fmt.Fprintln(&details, "Authentication method unspecified.")
	}

	if len(incorrect) > 0 {
		return errors.WithDetail(errors.Newf("URL validation error: %s", redact.Join(", ", incorrect)), details.String())
	}
	return nil
}
