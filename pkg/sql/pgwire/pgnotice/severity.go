// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgnotice

import (
	"fmt"
	"strings"
)

// Severity indicates the severity of a given error for the
// purposes of displaying notices.
// This corresponds to the allowed values for the `client_min_messages`
// variable in postgres.
type Severity int

const (
	// SeverityError is a Severity value allowing all notices
	// of value <= SeverityError to display.
	SeverityError = iota
	// SeverityWarning is a Severity value allowing all notices
	// of value <= SeverityWarning to display.
	SeverityWarning
	// SeverityNotice is a Severity value allowing all notices
	// of value <= SeverityNotice to display.
	SeverityNotice
	// SeverityLog is a Severity value allowing all notices
	// of value <= SeverityLog.g to display.
	SeverityLog
	// SeverityDebug1 is a Severity value allowing all notices
	// of value <= SeverityDebug1 to display.
	SeverityDebug1
	// SeverityDebug2 is a Severity value allowing all notices
	// of value <= SeverityDebug2 to display.
	SeverityDebug2
	// SeverityDebug3 is a Severity value allowing all notices
	// of value <= SeverityDebug3 to display.
	SeverityDebug3
	// SeverityDebug4 is a Severity value allowing all notices
	// of value <= SeverityDebug4 to display.
	SeverityDebug4
	// SeverityDebug5 is a Severity value allowing all notices
	// of value <= SeverityDebug5 to display.
	SeverityDebug5 Severity = iota
)

// ParseSeverity translates a string to a Severity.
// Returns the severity, and a bool indicating whether the severity exists.
func ParseSeverity(k string) (Severity, bool) {
	s, ok := namesToSeverity[strings.ToLower(k)]
	return s, ok
}

func (ns Severity) String() string {
	if ns < 0 || ns > Severity(len(noticeSeverityNames)-1) {
		return fmt.Sprintf("Severity(%d)", ns)
	}
	return noticeSeverityNames[ns]
}

// noticeSeverityNames maps a Severity into it's string representation.
var noticeSeverityNames = [...]string{
	SeverityDebug5:  "debug5",
	SeverityDebug4:  "debug4",
	SeverityDebug3:  "debug3",
	SeverityDebug2:  "debug2",
	SeverityDebug1:  "debug1",
	SeverityLog:     "log",
	SeverityNotice:  "notice",
	SeverityWarning: "warning",
	SeverityError:   "error",
}

// namesToSeverity is the reverse mapping from string to Severity.
var namesToSeverity = map[string]Severity{}

// ValidSeverities returns a list of all valid severities.
func ValidSeverities() []string {
	ret := make([]string, 0, len(namesToSeverity))
	for _, s := range noticeSeverityNames {
		ret = append(ret, s)
	}
	return ret
}

func init() {
	for k, v := range noticeSeverityNames {
		namesToSeverity[v] = Severity(k)
	}
}

// WireString returns the value to return over pgwire.
// TODO(otan): we should have the error proto have a "Severity" field,
// and have this information there. Severity should instead become
// a wrapper for only the `client_min_messages` session variable.
func (ns Severity) WireString() string {
	switch ns {
	case SeverityDebug5, SeverityDebug4, SeverityDebug3, SeverityDebug2, SeverityDebug1:
		return "DEBUG"
	case SeverityLog:
		return "LOG"
	case SeverityNotice:
		return "NOTICE"
	case SeverityWarning:
		return "WARNING"
	case SeverityError:
		return "ERROR"
	default:
		// Purposely return a severity string which looks wrong.
		return "??NOTICE??"
	}
}
