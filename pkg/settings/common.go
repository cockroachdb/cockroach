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
	description string
	visibility  Visibility
	systemOnly  bool
	// Each setting has a slotIdx which is used as a handle with Values.
	slotIdx       int
	nonReportable bool
	retired       bool
}

func (i *common) isRetired() bool {
	return i.retired
}

func (i *common) setSlotIdx(slotIdx int) {
	if slotIdx < 1 {
		panic(fmt.Sprintf("Invalid slot index %d", slotIdx))
	}
	if slotIdx > MaxSettings {
		panic("too many settings; increase MaxSettings")
	}
	i.slotIdx = slotIdx
}
func (i *common) getSlotIdx() int {
	return i.slotIdx
}

func (i *common) setDescription(s string) {
	i.description = s
}

func (i common) Description() string {
	return i.description
}

func (i common) Visibility() Visibility {
	return i.visibility
}

func (i common) SystemOnly() bool {
	return i.systemOnly
}

func (i common) isReportable() bool {
	return !i.nonReportable
}

func (i *common) ErrorHint() (bool, string) {
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
func (i *common) SetReportable(reportable bool) {
	i.nonReportable = !reportable
}

// SetVisibility customizes the visibility of a setting.
func (i *common) SetVisibility(v Visibility) {
	i.visibility = v
}

// SetRetired marks the setting as obsolete. It also hides
// it from the output of SHOW CLUSTER SETTINGS.
func (i *common) SetRetired() {
	i.description = "do not use - " + i.description
	i.retired = true
}

// SetOnChange installs a callback to be called when a setting's value changes.
// `fn` should avoid doing long-running or blocking work as it is called on the
// goroutine which handles all settings updates.
func (i *common) SetOnChange(sv *Values, fn func(ctx context.Context)) {
	sv.setOnChange(i.slotIdx, fn)
}

type internalSetting interface {
	NonMaskedSetting

	isRetired() bool
	setToDefault(ctx context.Context, sv *Values)
	setDescription(desc string)
	setSlotIdx(slotIdx int)
	getSlotIdx() int
	// isReportable indicates whether the value of the setting can be
	// included in user-facing reports such as that produced by SHOW ALL
	// CLUSTER SETTINGS.
	// This only affects reports though; direct access is unconstrained.
	// For example, `enterprise.license` is non-reportable:
	// it cannot be listed, but can be accessed with `SHOW CLUSTER
	// SETTING enterprise.license` or SET CLUSTER SETTING.
	isReportable() bool
}
