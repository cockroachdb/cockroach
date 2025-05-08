// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package statuspb

import (
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
)

func (m HealthCheckResult) String() string {
	return redact.StringWithoutMarkers(m)
}

func (m HealthCheckResult) SafeFormat(s interfaces.SafePrinter, verb rune) {
	s.SafeString("[")
	for i, al := range m.Alerts {
		if i > 0 {
			s.SafeString(", ")
		}
		s.Print(al)
	}
	s.SafeString("]")
}

func (m HealthAlert) String() string {
	return redact.StringWithoutMarkers(m)
}

func (m HealthAlert) SafeFormat(s interfaces.SafePrinter, verb rune) {
	s.SafeString("{name: \"")
	s.Print(m.SafeDescription)
	s.SafeString("\", value: ")
	s.SafeFloat(redact.SafeFloat(m.Value))
	s.SafeString(", store: ")
	s.Print(m.StoreID)
	s.SafeString("}")
}
