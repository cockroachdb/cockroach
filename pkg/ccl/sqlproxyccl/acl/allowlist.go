// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package acl

import (
	"context"
	"net"

	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v2"
)

type AllowlistFile struct {
	Seq       int64                 `yaml:"SequenceNumber"`
	Allowlist map[string]AllowEntry `yaml:"allowlist"`
}

// Allowlist represents the current IP Allowlist, which maps tenant IDs to a
// list of allowed IP ranges.
type Allowlist struct {
	entries map[string]AllowEntry
}

var _ AccessController = &Allowlist{}
var _ yaml.Unmarshaler = &Allowlist{}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (al *Allowlist) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var f AllowlistFile
	if err := unmarshal(&f); err != nil {
		return err
	}
	al.entries = f.Allowlist
	return nil
}

// CheckConnection implements the AccessController interface.
//
// TODO(jaylim-crl): Call LookupTenant and return nil if the cluster has no
// public connectivity. This ACL shouldn't be applied. We would need to do this
// eventually once we move IP allowlist entries into the tenant object. Don't
// need to do this now as we don't need anything from the tenant metadata.
func (al *Allowlist) CheckConnection(ctx context.Context, connection ConnectionTags) error {
	entry, ok := al.entries[connection.TenantID.String()]
	if !ok {
		// No allowlist entry, allow all traffic
		return nil
	}
	ip := net.ParseIP(connection.IP)
	if ip == nil {
		return errors.Newf("could not parse ip address: '%s'", ip)
	}
	// Check all ips for this cluster.
	// If one of them contains the current IP then it's allowed.
	for _, allowedIP := range entry.ips {
		if allowedIP.Contains(ip) {
			return nil
		}
	}

	return errors.Newf("connection ip '%s' denied: ip address not allowed", connection.IP)
}

type AllowEntry struct {
	ips []*net.IPNet
}

var _ yaml.Unmarshaler = &AllowEntry{}

// This custom unmarshal code converts each string IP address into a *net.IPNet.
// If it cannot be parsed, it is currently ignored and not added to the AllowEntry.
//
// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (e *AllowEntry) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var raw struct {
		IPs []string `yaml:"ips"`
	}

	if err := unmarshal(&raw); err != nil {
		return err
	}
	e.ips = make([]*net.IPNet, 0)

	for _, ip := range raw.IPs {
		_, ipNet, _ := net.ParseCIDR(ip)
		if ipNet != nil {
			e.ips = append(e.ips, ipNet)
		}
	}

	return nil
}
