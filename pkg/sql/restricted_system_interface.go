// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// TipUserAboutSystemInterface informs the user in error payloads
// about the existence of the system interface. This is a UX
// enhancement meant to facilitate the transition from single-tenant,
// non-virtualized CockroachDB to virtual clusters.
var TipUserAboutSystemInterface = settings.RegisterBoolSetting(
	settings.SystemVisible,
	"sql.error_tip_system_interface.enabled",
	"if enabled, certain errors contain a tip to use the system interface",
	false)

// maybeAddSystemInterfaceHint informs the user in error payloads
// about the existence of the system interface, if the cluster setting
// TipUserAboutSystemInterface is enabled. This is a UX enhancement
// meant to facilitate the transition from single-tenant,
// non-virtualized CockroachDB to virtual clusters.
func (p *planner) maybeAddSystemInterfaceHint(err error, operation redact.SafeString) error {
	return maybeAddSystemInterfaceHint(err, operation, p.ExecCfg().Codec, p.ExecCfg().Settings)
}

// maybeAddSystemInterfaceHint informs the user in error payloads
// about the existence of the system interface, if the cluster setting
// TipUserAboutSystemInterface is enabled. This is a UX enhancement
// meant to facilitate the transition from single-tenant,
// non-virtualized CockroachDB to virtual clusters.
func maybeAddSystemInterfaceHint(
	err error, operation redact.SafeString, codec keys.SQLCodec, st *cluster.Settings,
) error {
	if err == nil {
		return nil
	}
	forSystemTenant := codec.ForSystemTenant()
	tipSystemInterface := !forSystemTenant && TipUserAboutSystemInterface.Get(&st.SV)
	if !tipSystemInterface {
		return err
	}
	return errors.WithHintf(err, "Connect to the system interface and %s from there.", operation)
}

// RestrictAccessToSystemInterface restricts access to certain SQL
// features from the system tenant/interface. This restriction exists
// to prevent the following UX surprise:
//
//   - end-user desires to achieve a certain outcome in a virtual cluster;
//   - however, they mess up their connection string and connect to the
//     system tenant instead;
//   - without this setting, the resulting SQL would succeed in the
//     system tenant and the user would not realize they were not
//     connected to the right place.
var RestrictAccessToSystemInterface = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"sql.restrict_system_interface.enabled",
	"if enabled, certain statements produce errors or warnings when run from the system interface to encourage use of a virtual cluster",
	false)

// shouldRestrictAccessToSystemInterface decides whether to restrict
// access to certain SQL features from the system tenant/interface.
// This restriction exists to prevent UX surprise. See the docstring
// on the RestrictAccessToSystemInterface cluster setting for details.
func (p *planner) shouldRestrictAccessToSystemInterface(
	ctx context.Context, operation, alternateAction redact.RedactableString,
) error {
	if p.ExecCfg().Codec.ForSystemTenant() &&
		!p.EvalContext().SessionData().Internal && // We only restrict access for external SQL sessions.
		RestrictAccessToSystemInterface.Get(&p.ExecCfg().Settings.SV) {
		return errors.WithHintf(
			pgerror.Newf(pgcode.InsufficientPrivilege, "blocked %s from the system interface", operation),
			"Access blocked via %s to prevent likely user errors.\n"+
				"Try %s from a virtual cluster instead.",
			RestrictAccessToSystemInterface.Name(),
			alternateAction)
	}
	return nil
}
