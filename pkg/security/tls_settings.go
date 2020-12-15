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
}

var ocspMode = settings.RegisterEnumSetting("security.ocsp.mode",
	`use OCSP to check whether TLS certificates are revoked. If the OCSP
server is unreachable, in strict mode all certificates will be rejected
and in lax mode all certificates will be accepted.`,
	"off", map[int64]string{ocspOff: "off", ocspLax: "lax", ocspStrict: "strict"}).WithPublic()

// TODO(bdarnell): 3 seconds is the same as base.NetworkTimeout, but
// we can't use it here due to import cycles. We need a real
// no-dependencies base package for constants like this.
var ocspTimeout = settings.RegisterDurationSetting("security.ocsp.timeout",
	"timeout before considering the OCSP server unreachable",
	3*time.Second,
	settings.NonNegativeDuration,
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
