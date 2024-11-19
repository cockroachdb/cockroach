// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
)

const (
	ocspOff    = 0
	ocspLax    = 1
	ocspStrict = 2

	// OldCipherSuitesEnabledEnv is the environment variable used to reenable
	// use of old cipher suites for backwards compatibility with applications
	// that do not support any of the recommended cipher suites.
	OldCipherSuitesEnabledEnv = "COCKROACH_TLS_ENABLE_OLD_CIPHER_SUITES"
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
	settings.ApplicationLevel, "security.ocsp.mode",
	"use OCSP to check whether TLS certificates are revoked. If the OCSP "+
		"server is unreachable, in strict mode all certificates will be rejected "+
		"and in lax mode all certificates will be accepted.",
	"off", map[int64]string{ocspOff: "off", ocspLax: "lax", ocspStrict: "strict"},
	settings.WithPublic)

var ocspTimeout = settings.RegisterDurationSetting(
	settings.ApplicationLevel, "security.ocsp.timeout",
	"timeout before considering the OCSP server unreachable",
	3*time.Second,
	settings.NonNegativeDuration,
	settings.WithPublic)

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
	return areOldCipherSuitesEnabled()
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
	return areOldCipherSuitesEnabled()
}

// areOldCipherSuites returns true if CRDB should enable the use of
// old, no longer recommended TLS cipher suites for the sake of
// compatibility.
func areOldCipherSuitesEnabled() bool {
	return envutil.EnvOrDefaultBool(OldCipherSuitesEnabledEnv, false)
}
