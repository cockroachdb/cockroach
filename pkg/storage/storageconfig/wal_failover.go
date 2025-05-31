// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storageconfig

import (
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type WALFailoverMode int32

const (
	// WALFailoverDefaultMode leaves the WAL failover configuration unspecified.
	// Today this is interpreted as WALFailoverDisabled but future releases may
	// default to another mode.
	WALFailoverDefaultMode WALFailoverMode = 0
	// WALFailoverDisabled leaves WAL failover disabled. Commits to the storage
	// engine observe the latency of a store's primary WAL directly.
	WALFailoverDisabled WALFailoverMode = 1
	// WALFailoverAmongStores enables WAL failover among multiple stores within a
	// node. This setting has no effect if the node has a single store. When a
	// storage engine observes high latency writing to its WAL, it may
	// transparently failover to an arbitrary, predetermined other store's data
	// directory. If successful in syncing log entries to the other store's
	// volume, the batch commit latency is insulated from the effects of momentary
	// disk stalls.
	WALFailoverAmongStores WALFailoverMode = 2
	// WALFailoverToExplicitPath enables WAL failover for a single-store node to
	// an explicitly specified path.
	WALFailoverToExplicitPath WALFailoverMode = 3
)

// ExternalPath is a path with encryption options.
type ExternalPath struct {
	// The path to the directory.
	Path string
	// The encryption options for the directory. May be nil.
	Encryption *EncryptionOptions
}

// WALFailover is the configuration for write-ahead log (WAL) failover, used
// to temporarily write WALs to a separate location when disk
// stalls are encountered.
type WALFailover struct {
	Mode WALFailoverMode
	// Path is the non-store path to which WALs should be written when failing
	// over. It must be nonempty if and only if Mode ==
	// WALFailoverToExplicitPath.
	Path ExternalPath
	// PrevPath is the previously used non-store path. It may be set with Mode ==
	// WALFailoverToExplicitPath (when changing the secondary path) or
	// WALFailoverDisabled (when disabling WAL failover after it was
	// previously enabled with WALFailoverToExplicitPath). It must be empty
	// for other modes. If Mode is WALFailoverDisabled and previously WAL
	// failover was enabled using WALFailoverAmongStores, then PrevPath must
	// not be set.
	PrevPath ExternalPath
}

// Type implements the pflag.Value interface.
func (c *WALFailover) Type() string { return "string" }

// String implements fmt.Stringer.
func (c *WALFailover) String() string {
	return redact.StringWithoutMarkers(c)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (c *WALFailover) SafeFormat(p redact.SafePrinter, _ rune) {
	switch c.Mode {
	case WALFailoverDefaultMode:
		// Empty
	case WALFailoverDisabled:
		p.SafeString("disabled")
		if c.PrevPath.IsSet() {
			p.SafeString(",prev_path=")
			p.SafeString(redact.SafeString(c.PrevPath.Path))
		}
	case WALFailoverAmongStores:
		p.SafeString("among-stores")
	case WALFailoverToExplicitPath:
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
		c.Mode = WALFailoverDisabled
		var ok bool
		c.Path.Path, c.PrevPath.Path, ok = parseWALFailoverPathFields(strings.TrimPrefix(s, "disabled"))
		if !ok || c.Path.IsSet() {
			return errors.Newf("invalid disabled --wal-failover setting: %s "+
				"expect disabled[,prev_path=<prev_path>]", s)
		}
	case s == "among-stores":
		c.Mode = WALFailoverAmongStores
	case strings.HasPrefix(s, "path="):
		c.Mode = WALFailoverToExplicitPath
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
