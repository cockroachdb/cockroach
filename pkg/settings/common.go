// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settings

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
)

// common implements basic functionality used by all setting types.
type common struct {
	class         Class
	key           InternalKey
	name          SettingName
	description   string
	visibility    Visibility
	unsafe        bool
	slot          slotIdx
	nonReportable bool
	retired       bool
	sensitive     bool
}

// slotIdx is an integer in the range [0, MaxSetting) which is uniquely
// associated with a registered setting. Slot indexes are used as "handles" for
// manipulating the setting values. They are generated sequentially, in the
// order of registration.
type slotIdx int32

// init must be called to initialize the fields that don't have defaults.
func (c *common) init(class Class, key InternalKey, description string, slot slotIdx) {
	c.class = class
	c.key = key
	c.name = SettingName(key) // until overridden
	c.description = description
	if slot < 0 {
		panic(fmt.Sprintf("Invalid slot index %d", slot))
	}
	if slot >= MaxSettings {
		panic("too many settings; increase MaxSettings")
	}
	c.slot = slot
}

func (c common) Class() Class {
	return c.class
}

func (c common) InternalKey() InternalKey {
	return c.key
}

func (c common) Name() SettingName {
	return c.name
}

func (c common) Description() string {
	return c.description
}

func (c common) Visibility() Visibility {
	return c.visibility
}

func (c common) isReportable() bool {
	return !c.nonReportable
}

func (c *common) isRetired() bool {
	return c.retired
}

func (c *common) isSensitive() bool {
	return c.sensitive
}

func (c *common) ErrorHint() (bool, string) {
	return false, ""
}

func (c *common) getSlot() slotIdx {
	return c.slot
}

// ValueOrigin returns the origin of the current value of the setting.
func (c *common) ValueOrigin(ctx context.Context, sv *Values) ValueOrigin {
	return sv.getValueOrigin(ctx, c.slot)
}

// setReportable configures the reportability.
// Refer to the WithReportable option for details.
func (c *common) setReportable(reportable bool) {
	c.nonReportable = !reportable
}

// setVisibility customizes the visibility of a setting.
// Refer to the WithVisibility option for details.
func (c *common) setVisibility(v Visibility) {
	c.visibility = v
}

// setRetired marks the setting as obsolete. It also hides
// it from the output of SHOW CLUSTER SETTINGS.
func (c *common) setRetired() {
	c.description = "do not use - " + c.description
	c.retired = true
}

// setSensitive marks the setting as sensitive, which means that it will always
// be redacted in any output. It also makes the setting non-reportable.
func (c *common) setSensitive() {
	c.sensitive = true
	c.nonReportable = true
}

// setName is used to override the name of the setting.
// Refer to the WithName option for details.
func (c *common) setName(name SettingName) {
	if c.name != SettingName(c.key) {
		panic(errors.AssertionFailedf("duplicate use of WithName"))
	}
	c.name = name
}

// setUnsafe is used to override the unsafe status of the setting.
// Refer to the WithUnsafe option for details.
func (c *common) setUnsafe() {
	c.unsafe = true
}

// IsUnsafe indicates whether the setting is unsafe.
func (c *common) IsUnsafe() bool {
	return c.unsafe
}

// SetOnChange installs a callback to be called when a setting's value changes.
// `fn` should avoid doing long-running or blocking work as it is called on the
// goroutine which handles all settings updates.
func (c *common) SetOnChange(sv *Values, fn func(ctx context.Context)) {
	sv.setOnChange(c.slot, fn)
}

type internalSetting interface {
	NonMaskedSetting

	init(class Class, key InternalKey, description string, slot slotIdx)
	isRetired() bool
	isSensitive() bool
	getSlot() slotIdx

	// isReportable indicates whether the value of the setting can be
	// included in user-facing reports such as that produced by SHOW ALL
	// CLUSTER SETTINGS.
	// This only affects reports though; direct access is unconstrained.
	// For example, `enterprise.license` is non-reportable:
	// it cannot be listed, but can be accessed with `SHOW CLUSTER
	// SETTING enterprise.license` or SET CLUSTER SETTING.
	isReportable() bool

	setToDefault(ctx context.Context, sv *Values)

	// decodeAndSet sets the setting after decoding the encoded value.
	decodeAndSet(ctx context.Context, sv *Values, encoded string) error

	// decodeAndOverrideDefault overrides the default value for the respective
	// setting to newVal. It does not change the current value. Validation checks
	// are not run against the decoded value.
	decodeAndSetDefaultOverride(ctx context.Context, sv *Values, encoded string) error
}

// TestingIsReportable is used in testing for reportability.
func TestingIsReportable(s Setting) bool {
	if _, ok := s.(*MaskedSetting); ok {
		return false
	}
	if e, ok := s.(internalSetting); ok {
		return e.isReportable()
	}
	return true
}

// TestingIsSensitive is used in testing for sensitivity.
func TestingIsSensitive(s Setting) bool {
	if e, ok := s.(internalSetting); ok {
		return e.isSensitive()
	}
	return false
}
