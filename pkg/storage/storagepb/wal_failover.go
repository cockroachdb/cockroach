// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storagepb

import (
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// Type implements the pflag.Value interface.
func (c *WALFailover) Type() string { return "string" }

// String implements fmt.Stringer.
func (c *WALFailover) String() string {
	return redact.StringWithoutMarkers(c)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (c *WALFailover) SafeFormat(p redact.SafePrinter, _ rune) {
	switch c.Mode {
	case WALFailoverMode_DEFAULT:
		// Empty
	case WALFailoverMode_DISABLED:
		p.SafeString("disabled")
		if c.PrevPath.IsSet() {
			p.SafeString(",prev_path=")
			p.SafeString(redact.SafeString(c.PrevPath.Path))
		}
	case WALFailoverMode_AMONG_STORES:
		p.SafeString("among-stores")
	case WALFailoverMode_EXPLICIT_PATH:
		p.SafeString("path=")
		p.SafeString(redact.SafeString(c.Path.Path))
		if c.PrevPath.IsSet() {
			p.SafeString(",prev_path=")
			p.SafeString(redact.SafeString(c.PrevPath.Path))
		}
	default:
		p.Printf("<unknown WALFailoverMode %d>", int8(c.Mode))
	}
}

// Set implements the pflag.Value interface.
func (c *WALFailover) Set(s string) error {
	switch {
	case strings.HasPrefix(s, "disabled"):
		c.Mode = WALFailoverMode_DISABLED
		var ok bool
		c.Path.Path, c.PrevPath.Path, ok = parseWALFailoverPathFields(strings.TrimPrefix(s, "disabled"))
		if !ok || c.Path.IsSet() {
			return errors.Newf("invalid disabled --wal-failover setting: %s "+
				"expect disabled[,prev_path=<prev_path>]", s)
		}
	case s == "among-stores":
		c.Mode = WALFailoverMode_AMONG_STORES
	case strings.HasPrefix(s, "path="):
		c.Mode = WALFailoverMode_EXPLICIT_PATH
		var ok bool
		c.Path.Path, c.PrevPath.Path, ok = parseWALFailoverPathFields(s)
		if !ok || !c.Path.IsSet() {
			return errors.Newf("invalid path --wal-failover setting: %s "+
				"expect path=<path>[,prev_path=<prev_path>]", s)
		}
	default:
		return errors.Newf("invalid --wal-failover setting: %s "+
			"(possible values: disabled, among-stores, path=<path>)", s)
	}
	return nil
}

func parseWALFailoverPathFields(s string) (path, prevPath string, ok bool) {
	if s == "" {
		return "", "", true
	}
	if s2 := strings.TrimPrefix(s, "path="); len(s2) < len(s) {
		s = s2
		if i := strings.IndexByte(s, ','); i == -1 {
			return s, "", true
		} else {
			path = s[:i]
			s = s[i:]
		}
	}

	// Any remainder must be a prev_path= field.
	if !strings.HasPrefix(s, ",prev_path=") {
		return "", "", false
	}
	prevPath = strings.TrimPrefix(s, ",prev_path=")
	return path, prevPath, true
}

// IsSet returns whether or not the path was provided.
func (e ExternalPath) IsSet() bool { return e.Path != "" }
