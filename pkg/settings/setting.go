// Copyright 2017 The Cockroach Authors.
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
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// MaxSettings is the maximum number of settings that the system supports.
// Exported for tests.
const MaxSettings = 512

// Values is a container that stores values for all registered settings.
// Each setting is assigned a unique slot (up to MaxSettings).
// Note that slot indices are 1-based (this is to trigger panics if an
// uninitialized slot index is used).
type Values struct {
	container valuesContainer

	overridesMu struct {
		syncutil.Mutex
		// defaultOverrides maintains the set of overridden default values (see
		// Override()).
		defaultOverrides valuesContainer
		// setOverrides is the list of slots with values in defaultOverrides.
		setOverrides map[int]struct{}
	}

	changeMu struct {
		syncutil.Mutex
		// NB: any in place modification to individual slices must also hold the
		// lock, e.g. if we ever add RemoveOnChange or something.
		onChange [MaxSettings][]func(ctx context.Context)
	}
	// opaque is an arbitrary object that can be set by a higher layer to make it
	// accessible from certain callbacks (like state machine transformers).
	opaque interface{}
}

type valuesContainer struct {
	intVals     [MaxSettings]int64
	genericVals [MaxSettings]atomic.Value
}

func (c *valuesContainer) setGenericVal(slotIdx int, newVal interface{}) {
	c.genericVals[slotIdx].Store(newVal)
}

func (c *valuesContainer) setInt64Val(slotIdx int, newVal int64) bool {
	return atomic.SwapInt64(&c.intVals[slotIdx], newVal) != newVal
}

var (
	canonicalValues atomic.Value
)

// TODO is usable at callsites that do not have *settings.Values available.
// Please don't use this.
func TODO() *Values {
	if ptr := canonicalValues.Load(); ptr != nil {
		return ptr.(*Values)
	}
	return nil
}

// SetCanonicalValuesContainer sets the Values container that will be refreshed
// at runtime -- ideally we should have no other *Values containers floating
// around, as they will be stale / lies.
func SetCanonicalValuesContainer(v *Values) {
	canonicalValues.Store(v)
}

type testOpaqueType struct{}

// TestOpaque can be passed to Values.Init when we are testing the settings
// infrastructure.
var TestOpaque interface{} = testOpaqueType{}

// Init must be called before using a Values instance; it initializes all
// variables to their defaults.
//
// The opaque argument can be retrieved later via Opaque().
func (sv *Values) Init(ctx context.Context, opaque interface{}) {
	sv.opaque = opaque
	for _, s := range registry {
		s.setToDefault(ctx, sv)
	}
}

// Opaque returns the argument passed to Init.
func (sv *Values) Opaque() interface{} {
	return sv.opaque
}

func (sv *Values) settingChanged(ctx context.Context, slotIdx int) {
	sv.changeMu.Lock()
	funcs := sv.changeMu.onChange[slotIdx-1]
	sv.changeMu.Unlock()
	for _, fn := range funcs {
		fn(ctx)
	}
}

func (c *valuesContainer) getInt64(slotIdx int) int64 {
	return atomic.LoadInt64(&c.intVals[slotIdx-1])
}

func (c *valuesContainer) getGeneric(slotIdx int) interface{} {
	return c.genericVals[slotIdx-1].Load()
}

func (sv *Values) setInt64(ctx context.Context, slotIdx int, newVal int64) {
	if sv.container.setInt64Val(slotIdx-1, newVal) {
		sv.settingChanged(ctx, slotIdx)
	}
}

// setDefaultOverrideInt64 overrides the default value for the respective
// setting to newVal.
func (sv *Values) setDefaultOverrideInt64(slotIdx int, newVal int64) {
	sv.overridesMu.Lock()
	defer sv.overridesMu.Unlock()
	sv.overridesMu.defaultOverrides.setInt64Val(slotIdx-1, newVal)
	sv.setDefaultOverrideLocked(slotIdx)
}

// setDefaultOverrideLocked marks slotIdx-1 as having an overridden default value.
func (sv *Values) setDefaultOverrideLocked(slotIdx int) {
	if sv.overridesMu.setOverrides == nil {
		sv.overridesMu.setOverrides = make(map[int]struct{})
	}
	sv.overridesMu.setOverrides[slotIdx-1] = struct{}{}
}

// getDefaultOverrides checks whether there's a default override for slotIdx-1.
// If there isn't, the first ret val is false. Otherwise, the first ret val is
// true, the second is the int64 override and the last is a pointer to the
// generic value override. Callers are expected to only use the override value
// corresponding to their setting type.
func (sv *Values) getDefaultOverride(slotIdx int) (bool, int64, *atomic.Value) {
	slotIdx--
	sv.overridesMu.Lock()
	defer sv.overridesMu.Unlock()
	if _, ok := sv.overridesMu.setOverrides[slotIdx]; !ok {
		return false, 0, nil
	}
	return true,
		sv.overridesMu.defaultOverrides.intVals[slotIdx],
		&sv.overridesMu.defaultOverrides.genericVals[slotIdx]
}

func (sv *Values) setGeneric(ctx context.Context, slotIdx int, newVal interface{}) {
	sv.container.setGenericVal(slotIdx-1, newVal)
	sv.settingChanged(ctx, slotIdx)
}

func (sv *Values) getInt64(slotIdx int) int64 {
	return sv.container.getInt64(slotIdx)
}

func (sv *Values) getGeneric(slotIdx int) interface{} {
	return sv.container.getGeneric(slotIdx)
}

// setOnChange installs a callback to be called when a setting's value changes.
// `fn` should avoid doing long-running or blocking work as it is called on the
// goroutine which handles all settings updates.
func (sv *Values) setOnChange(slotIdx int, fn func(ctx context.Context)) {
	sv.changeMu.Lock()
	sv.changeMu.onChange[slotIdx-1] = append(sv.changeMu.onChange[slotIdx-1], fn)
	sv.changeMu.Unlock()
}

// Setting is a descriptor for each setting; once it is initialized, it is
// immutable. The values for the settings are stored separately, in
// Values. This way we can have a global set of registered settings, each
// with potentially multiple instances.
type Setting interface {
	// Typ returns the short (1 char) string denoting the type of setting.
	Typ() string
	// String returns the string representation of the setting's current value.
	// It's used when materializing results for `SHOW CLUSTER SETTINGS` or `SHOW
	// CLUSTER SETTING <setting-name>`.
	String(sv *Values) string
	// Description contains a helpful text explaining what the specific cluster
	// setting is for.
	Description() string
	// Visibility controls whether or not the setting is made publicly visible.
	// Reserved settings are still accessible to users, but they don't get
	// listed out when retrieving all settings.
	Visibility() Visibility

	// SystemOnly indicates if a setting is only applicable to the system tenant.
	SystemOnly() bool
}

// WritableSetting is the exported interface of non-masked settings.
type WritableSetting interface {
	Setting

	// Encoded returns the encoded representation of the current value of the
	// setting.
	Encoded(sv *Values) string
	// EncodedDefault returns the encoded representation of the default value of
	// the setting.
	EncodedDefault() string
	// SetOnChange installs a callback to be called when a setting's value
	// changes. `fn` should avoid doing long-running or blocking work as it is
	// called on the goroutine which handles all settings updates.
	SetOnChange(sv *Values, fn func(ctx context.Context))
	// ErrorHint returns a hint message to be displayed to the user when there's
	// an error.
	ErrorHint() (bool, string)
}

type extendedSetting interface {
	WritableSetting

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

// Visibility describes how a user should feel confident that
// they can customize the setting.  See the constant definitions below
// for details.
type Visibility int

const (
	// Reserved - which is the default - indicates that a setting is
	// not documented and the CockroachDB team has not developed
	// internal experience about the impact of customizing it to other
	// values.
	// In short: "Use at your own risk."
	Reserved Visibility = iota
	// Public indicates that a setting is documented, the range of
	// possible values yields predictable results, and the CockroachDB
	// team is there to assist if issues occur as a result of the
	// customization.
	// In short: "Go ahead but be careful."
	Public
)

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

type numericSetting interface {
	Setting
	Validate(i int64) error
	set(ctx context.Context, sv *Values, i int64) error
}

// TestingIsReportable is used in testing for reportability.
func TestingIsReportable(s Setting) bool {
	if _, ok := s.(*MaskedSetting); ok {
		return false
	}
	if e, ok := s.(extendedSetting); ok {
		return e.isReportable()
	}
	return true
}

// AdminOnly returns whether the setting can only be viewed and modified by
// superusers. Otherwise, users with the MODIFYCLUSTERSETTING role privilege can
// do so.
func AdminOnly(name string) bool {
	return !strings.HasPrefix(name, "sql.defaults.")
}
