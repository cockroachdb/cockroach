// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverpb

import (
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
)

var (
	eventSetClusterSettingName       = logpb.GetEventTypeName(&eventpb.SetClusterSetting{})
	eventSetTenantClusterSettingName = logpb.GetEventTypeName(&eventpb.SetTenantClusterSetting{})
)

// RedactEventInfo returns the redacted form of a system.eventlog info
// payload, as served by the admin Events API by default: the statement text
// and bound placeholder values are hidden for every event type, and the new
// value is additionally hidden for setting-change events. Placeholder values
// are hidden along with the statement text because they are fragments of the
// statement and can carry secrets (e.g. a bound password or sensitive
// cluster setting value) that the statement text itself hides. A payload
// that does not parse as JSON offers no way to locate secrets within it, so
// it is dropped entirely.
//
// The server applies this when serving the redacted form, and the debug zip
// client re-applies it to the response (see scrubEventsResponse in pkg/cli)
// so that events.json is clean even when collected from a server that
// predates parts of this redaction.
func RedactEventInfo(eventType, info string) string {
	s := map[string]interface{}{}
	if err := json.Unmarshal([]byte(info), &s); err != nil {
		return ""
	}
	if _, ok := s["Statement"]; ok {
		s["Statement"] = "<hidden>"
	}
	if placeholders, ok := s["PlaceholderValues"].([]interface{}); ok {
		for i := range placeholders {
			placeholders[i] = "<hidden>"
		}
	}
	if eventType == eventSetClusterSettingName ||
		eventType == eventSetTenantClusterSettingName {
		if _, ok := s["Value"]; ok {
			s["Value"] = "<hidden>"
		}
	}
	ret, err := json.Marshal(s)
	if err != nil {
		return ""
	}
	return string(ret)
}
