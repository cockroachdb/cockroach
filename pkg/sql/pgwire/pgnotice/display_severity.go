// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgnotice

import (
	"fmt"
	"strings"
)

// DisplaySeverity indicates the severity of a given error for the
// purposes of displaying notices.
// This corresponds to the allowed values for the `client_min_messages`
// variable in postgres.
type DisplaySeverity uint32

// It is important to keep the same order here as Postgres.
// See https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-CLIENT-MIN-MESSAGES.

const (
	// DisplaySeverityError is a DisplaySeverity value allowing all notices
	// of value <= DisplaySeverityError to display.
	DisplaySeverityError = iota
	// DisplaySeverityWarning is a DisplaySeverity value allowing all notices
	// of value <= DisplaySeverityWarning to display.
	DisplaySeverityWarning
	// DisplaySeverityNotice is a DisplaySeverity value allowing all notices
	// of value <= DisplaySeverityNotice to display.
	DisplaySeverityNotice
	// DisplaySeverityInfo is a DisplaySeverity value allowing all notices
	// of value <= DisplaySeverityInfo to display. DisplaySeverityInfo is a
	// special case in that it will always send a message to the client, no matter
	// the value of client_min_messages.
	DisplaySeverityInfo
	// DisplaySeverityLog is a DisplaySeverity value allowing all notices
	// of value <= DisplaySeverityLog to display.
	DisplaySeverityLog
	// DisplaySeverityDebug1 is a DisplaySeverity value allowing all notices
	// of value <= DisplaySeverityDebug1 to display.
	DisplaySeverityDebug1
	// DisplaySeverityDebug2 is a DisplaySeverity value allowing all notices
	// of value <= DisplaySeverityDebug2 to display.
	DisplaySeverityDebug2
	// DisplaySeverityDebug3 is a DisplaySeverity value allowing all notices
	// of value <= DisplaySeverityDebug3 to display.
	DisplaySeverityDebug3
	// DisplaySeverityDebug4 is a DisplaySeverity value allowing all notices
	// of value <= DisplaySeverityDebug4 to display.
	DisplaySeverityDebug4
	// DisplaySeverityDebug5 is a DisplaySeverity value allowing all notices
	// of value <= DisplaySeverityDebug5 to display.
	DisplaySeverityDebug5
)

// ParseDisplaySeverity translates a string to a DisplaySeverity.
// Returns the severity, and a bool indicating whether the severity exists.
func ParseDisplaySeverity(k string) (DisplaySeverity, bool) {
	s, ok := namesToDisplaySeverity[strings.ToLower(k)]
	return s, ok
}

func (ns DisplaySeverity) String() string {
	if ns >= DisplaySeverity(len(noticeDisplaySeverityNames)) {
		return fmt.Sprintf("DisplaySeverity(%d)", ns)
	}
	return noticeDisplaySeverityNames[ns]
}

// noticeDisplaySeverityNames maps a DisplaySeverity into it's string representation.
var noticeDisplaySeverityNames = [...]string{
	DisplaySeverityDebug5:  "debug5",
	DisplaySeverityDebug4:  "debug4",
	DisplaySeverityDebug3:  "debug3",
	DisplaySeverityDebug2:  "debug2",
	DisplaySeverityDebug1:  "debug1",
	DisplaySeverityLog:     "log",
	DisplaySeverityInfo:    "info",
	DisplaySeverityNotice:  "notice",
	DisplaySeverityWarning: "warning",
	DisplaySeverityError:   "error",
}

// namesToDisplaySeverity is the reverse mapping from string to DisplaySeverity.
var namesToDisplaySeverity = map[string]DisplaySeverity{}

// ValidDisplaySeverities returns a list of all valid severities.
func ValidDisplaySeverities() []string {
	ret := make([]string, 0, len(namesToDisplaySeverity))
	for _, s := range noticeDisplaySeverityNames {
		ret = append(ret, s)
	}
	return ret
}

func init() {
	for k, v := range noticeDisplaySeverityNames {
		namesToDisplaySeverity[v] = DisplaySeverity(k)
	}
}
