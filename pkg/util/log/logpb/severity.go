// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logpb

import (
	"strconv"
	"strings"
)

// Set is part of the pflag.Value interface.
func (s *Severity) Set(value string) error {
	var threshold Severity
	// Is it a known name?
	if v, ok := SeverityByName(value); ok {
		threshold = v
	} else {
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		threshold = Severity(v)
	}
	*s = threshold
	return nil
}

// IsSet returns true iff the severity was set to a non-unknown value.
func (s Severity) IsSet() bool { return s != Severity_UNKNOWN }

// Type implements the pflag.Value interface.
func (s Severity) Type() string { return "<severity>" }

// Name returns the string representation of the severity (i.e. ERROR, INFO).
func (s *Severity) Name() string {
	return s.String()
}

// SeverityByName attempts to parse the passed in string into a severity. (i.e.
// ERROR, INFO). If it succeeds, the returned bool is set to true.
func SeverityByName(s string) (Severity, bool) {
	s = strings.ToUpper(s)
	if i, ok := Severity_value[s]; ok {
		return Severity(i), true
	}
	switch s {
	case "TRUE":
		return Severity_INFO, true
	case "FALSE":
		return Severity_NONE, true
	}
	return 0, false
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (s *Severity) UnmarshalYAML(fn func(interface{}) error) error {
	var sv string
	if err := fn(&sv); err != nil {
		return err
	}
	return s.Set(sv)
}

// MarshalYAML implements the yaml.Marshaler interface.
func (s Severity) MarshalYAML() (interface{}, error) {
	return s.String(), nil
}
