// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package settings

// MaskedSetting is a pseudo-variable constructed on-the-fly by Lookup
// when the actual setting is non-reportable.
type MaskedSetting struct {
	setting WritableSetting
}

var _ Setting = &MaskedSetting{}

// UnderlyingSetting retrieves the actual setting object.
func (s *MaskedSetting) UnderlyingSetting() WritableSetting {
	return s.setting
}

// String hides the underlying value.
func (s *MaskedSetting) String(sv *Values) string {
	// Special case for non-reportable strings: we still want
	// to distinguish empty from non-empty (= customized).
	if st, ok := s.UnderlyingSetting().(*StringSetting); ok && st.String(sv) == "" {
		return ""
	}
	return "<redacted>"
}

// Visibility returns the visibility setting for the underlying setting.
func (s *MaskedSetting) Visibility() Visibility {
	return s.setting.Visibility()
}

// Description returns the description string for the underlying setting.
func (s *MaskedSetting) Description() string {
	return s.setting.Description()
}

// Typ returns the short (1 char) string denoting the type of setting.
func (s *MaskedSetting) Typ() string {
	return s.setting.Typ()
}

// SystemOnly returns the underlying setting's SystemOnly.
func (s *MaskedSetting) SystemOnly() bool {
	return s.setting.SystemOnly()
}
