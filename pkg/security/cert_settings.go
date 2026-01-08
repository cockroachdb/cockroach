// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security

import "github.com/cockroachdb/cockroach/pkg/settings"

// All cluster settings necessary for tls client cert authentication.
const (
	baseClientCertSettingName            = "security.client_cert."
	ClientCertSubjectRequiredSettingName = baseClientCertSettingName + "subject_required.enabled"
	ClientCertSANRequiredSettingName     = baseClientCertSettingName + "san_required.enabled"
)

// ClientCertSubjectRequired mandates a requirement for role subject to be set
// either through subject role option or root-cert-distinguished-name and
// node-cert-distinguished-name. It controls both RPC access and login via
// authCert
var ClientCertSubjectRequired = settings.RegisterBoolSetting(
	settings.SystemVisible,
	ClientCertSubjectRequiredSettingName,
	"mandates a requirement for subject role to be set for db user",
	false,
	settings.WithPublic,
	settings.WithReportable(true),
)

// ClientCertSANRequired mandates a requirement for SAN to be set
// in client certs. It controls both RPC access and login via
// authCert.
var ClientCertSANRequired = settings.RegisterBoolSetting(
	settings.SystemVisible,
	ClientCertSANRequiredSettingName,
	"mandates a requirement for client certs to contain SAN",
	false,
	settings.WithVisibility(settings.Reserved),
	settings.WithReportable(false),
)
