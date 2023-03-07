// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package acl

import (
	"net"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type AllowlistFile struct {
	Seq       int64                 `yaml:"SequenceNumber"`
	Allowlist map[string]AllowEntry `yaml:"allowlist"`
}

// Allowlist represents the current IP Allowlist,
// which maps cluster IDs to a list of allowed IP ranges.
type Allowlist struct {
	entries map[string]AllowEntry
}

func (al *Allowlist) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var f AllowlistFile
	if err := unmarshal(&f); err != nil {
		return err
	}
	al.entries = f.Allowlist
	return nil
}

func (al *Allowlist) CheckConnection(
	connection ConnectionTags, timeSource timeutil.TimeSource,
) error {
	entry, ok := al.entries[connection.Cluster]
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

// This custom unmarshal code converts each string IP address into a *net.IPNet.
// If it cannot be parsed, it is currently ignored and not added to the AllowEntry.
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
