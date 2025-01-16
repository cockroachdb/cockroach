// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settings

import "context"

// MaskedSetting is a wrapper for non-reportable settings that were retrieved
// for reporting (see SetReportable and LookupForReportingByKey).
type MaskedSetting struct {
	setting NonMaskedSetting
}

var _ Setting = &MaskedSetting{}

// redactSensitiveSettingsEnabled enables or disables the redaction of sensitive
// cluster settings.
var redactSensitiveSettingsEnabled = RegisterBoolSetting(
	ApplicationLevel,
	"server.redact_sensitive_settings.enabled",
	"enables or disables the redaction of sensitive settings in the output of SHOW CLUSTER SETTINGS and "+
		"SHOW ALL CLUSTER SETTINGS for users without the MODIFYCLUSTERSETTING privilege",
	false,
	WithPublic,
)

// String hides the underlying value.
func (s *MaskedSetting) String(sv *Values) string {
	// Special case for non-reportable/sensitive strings: we still want
	// to distinguish empty from non-empty (= customized).
	if st, ok := s.setting.(*StringSetting); ok && st.String(sv) == "" {
		return ""
	}
	isSensitive := false
	if st, ok := s.setting.(internalSetting); ok {
		isSensitive = st.isSensitive()
	}
	sensitiveRedactionEnabled := redactSensitiveSettingsEnabled.Get(sv)
	// Non-reportable settings are always redacted. Sensitive settings are
	// redacted if the redaction cluster setting is enabled.
	if !isSensitive || sensitiveRedactionEnabled {
		return "<redacted>"
	}
	return s.setting.String(sv)
}

// DefaultString returns the default value for the setting as a string.
func (s *MaskedSetting) DefaultString() string {
	return s.setting.DefaultString()
}

// Visibility returns the visibility setting for the underlying setting.
func (s *MaskedSetting) Visibility() Visibility {
	return s.setting.Visibility()
}

// InternalKey returns the key string for the underlying setting.
func (s *MaskedSetting) InternalKey() InternalKey {
	return s.setting.InternalKey()
}

// Name returns the name string for the underlying setting.
func (s *MaskedSetting) Name() SettingName {
	return s.setting.Name()
}

// Description returns the description string for the underlying setting.
func (s *MaskedSetting) Description() string {
	return s.setting.Description()
}

// Typ returns the short (1 char) string denoting the type of setting.
func (s *MaskedSetting) Typ() string {
	return s.setting.Typ()
}

// Class returns the class for the underlying setting.
func (s *MaskedSetting) Class() Class {
	return s.setting.Class()
}

// ValueOrigin returns the origin of the current value of the setting.
func (s *MaskedSetting) ValueOrigin(ctx context.Context, sv *Values) ValueOrigin {
	return s.setting.ValueOrigin(ctx, sv)
}

// IsUnsafe returns whether the underlying setting is unsafe.
func (s *MaskedSetting) IsUnsafe() bool {
	return s.setting.IsUnsafe()
}
