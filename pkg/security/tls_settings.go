// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

const (
	ocspOff    = 0
	ocspLax    = 1
	ocspStrict = 2
)

// TLSSettings allows for customization of TLS behavior. It's called
// "settings" instead of "config" to avoid ambiguity with the standard
// library's tls.Config. It is backed by cluster settings in a running
// node, but may be configured differently in CLI tools.
type TLSSettings interface {
	ocspEnabled() bool
	ocspStrict() bool
	ocspTimeout() time.Duration
	oldCipherSuitesEnabled() bool
}

var ocspMode = settings.RegisterEnumSetting(
	settings.TenantWritable, "security.ocsp.mode",
	"use OCSP to check whether TLS certificates are revoked. If the OCSP "+
		"server is unreachable, in strict mode all certificates will be rejected "+
		"and in lax mode all certificates will be accepted.",
	"off", map[int64]string{ocspOff: "off", ocspLax: "lax", ocspStrict: "strict"}).WithPublic()

var ocspTimeout = settings.RegisterDurationSetting(
	settings.TenantWritable, "security.ocsp.timeout",
	"timeout before considering the OCSP server unreachable",
	3*time.Second,
	settings.NonNegativeDuration,
).WithPublic()

var oldCipherSuitesEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly, "security.tls.useOldCipers",
	"enable the use of \"old\" cipher suites, strictly for use with "+
		"applications that support TLS v1.2 and cannot be updated to"+
		"support \"modern\" cipher suites",
	false,
).WithPublic()

type clusterTLSSettings struct {
	settings *cluster.Settings
}

var _ TLSSettings = clusterTLSSettings{}

func (c clusterTLSSettings) ocspEnabled() bool {
	return ocspMode.Get(&c.settings.SV) != ocspOff
}

func (c clusterTLSSettings) ocspStrict() bool {
	return ocspMode.Get(&c.settings.SV) == ocspStrict
}

func (c clusterTLSSettings) ocspTimeout() time.Duration {
	return ocspTimeout.Get(&c.settings.SV)
}

func (c clusterTLSSettings) oldCipherSuitesEnabled() bool {
	return oldCipherSuitesEnabled.Get(&c.settings.SV)
}

// ClusterTLSSettings creates a TLSSettings backed by the
// given cluster settings.
func ClusterTLSSettings(settings *cluster.Settings) TLSSettings {
	return clusterTLSSettings{settings}
}

// CommandTLSSettings defines the TLS settings for command-line tools.
// OCSP is not currently supported in this mode.
type CommandTLSSettings struct{}

var _ TLSSettings = CommandTLSSettings{}

func (CommandTLSSettings) ocspEnabled() bool {
	return false
}

func (CommandTLSSettings) ocspStrict() bool {
	return false
}

func (CommandTLSSettings) ocspTimeout() time.Duration {
	return 0
}

func (c CommandTLSSettings) oldCipherSuitesEnabled() bool {
	return false
}
