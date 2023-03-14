// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package settings

import (
	"context"
	"fmt"
)

// common implements basic functionality used by all setting types.
type common struct {
	class         Class
	key           string
	description   string
	visibility    Visibility
	slot          slotIdx
	nonReportable bool
	retired       bool
}

// slotIdx is an integer in the range [0, MaxSetting) which is uniquely
// associated with a registered setting. Slot indexes are used as "handles" for
// manipulating the setting values. They are generated sequentially, in the
// order of registration.
type slotIdx int32

// init must be called to initialize the fields that don't have defaults.
func (c *common) init(class Class, key string, description string, slot slotIdx) {
	c.class = class
	c.key = key
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

func (c common) Key() string {
	return c.key
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

func (c *common) ErrorHint() (bool, string) {
	return false, ""
}

// SetReportable indicates whether a setting's value can show up in SHOW ALL
// CLUSTER SETTINGS and telemetry reports.
//
// The setting can still be used with SET and SHOW if the exact
// setting name is known. Use SetReportable(false) for data that must
// be hidden from standard setting report, telemetry and
// troubleshooting screenshots, such as license data or keys.
//
// All string settings are also non-reportable by default and must be
// opted in to reports manually with SetReportable(true).
func (c *common) SetReportable(reportable bool) {
	c.nonReportable = !reportable
}

// SetVisibility customizes the visibility of a setting.
func (c *common) SetVisibility(v Visibility) {
	c.visibility = v
}

// SetRetired marks the setting as obsolete. It also hides
// it from the output of SHOW CLUSTER SETTINGS.
func (c *common) SetRetired() {
	c.description = "do not use - " + c.description
	c.retired = true
}

// SetOnChange installs a callback to be called when a setting's value changes.
// `fn` should avoid doing long-running or blocking work as it is called on the
// goroutine which handles all settings updates.
func (c *common) SetOnChange(sv *Values, fn func(ctx context.Context)) {
	sv.setOnChange(c.slot, fn)
}

func (c *common) slotIdx() slotIdx {
	return c.slot
}

type internalSetting interface {
	NonMaskedSetting

	init(class Class, key, description string, slot slotIdx)
	isRetired() bool
	setToDefault(ctx context.Context, sv *Values)

	// isReportable indicates whether the value of the setting can be
	// included in user-facing reports such as that produced by SHOW ALL
	// CLUSTER SETTINGS.
	// This only affects reports though; direct access is unconstrained.
	// For example, `enterprise.license` is non-reportable:
	// it cannot be listed, but can be accessed with `SHOW CLUSTER
	// SETTING enterprise.license` or SET CLUSTER SETTING.
	isReportable() bool
}

// numericSetting is used for settings that can be set using an integer value.
type numericSetting interface {
	internalSetting
	DecodeValue(value string) (int64, error)
	set(ctx context.Context, sv *Values, value int64) error
}
