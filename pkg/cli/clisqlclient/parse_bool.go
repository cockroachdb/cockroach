// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlclient

import (
	"strings"

	"github.com/cockroachdb/errors"
)

// ParseBool parses a boolean string for use in CLI SQL commands.
// It recognizes booleans in a similar way as 'psql'.
func ParseBool(s string) (bool, error) {
	switch strings.TrimSpace(strings.ToLower(s)) {
	case "true", "on", "yes", "1":
		return true, nil
	case "false", "off", "no", "0":
		return false, nil
	default:
		return false, errors.Newf("invalid boolean value %q", s)
	}
}
