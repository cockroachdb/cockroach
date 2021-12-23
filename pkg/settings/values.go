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
		setOverrides map[slotIdx]struct{}
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

func (c *valuesContainer) setGenericVal(slot slotIdx, newVal interface{}) {
	c.genericVals[slot].Store(newVal)
}

func (c *valuesContainer) setInt64Val(slot slotIdx, newVal int64) bool {
	return atomic.SwapInt64(&c.intVals[slot], newVal) != newVal
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

func (sv *Values) settingChanged(ctx context.Context, slot slotIdx) {
	sv.changeMu.Lock()
	funcs := sv.changeMu.onChange[slot]
	sv.changeMu.Unlock()
	for _, fn := range funcs {
		fn(ctx)
	}
}

func (c *valuesContainer) getInt64(slot slotIdx) int64 {
	return atomic.LoadInt64(&c.intVals[slot])
}

func (c *valuesContainer) getGeneric(slot slotIdx) interface{} {
	return c.genericVals[slot].Load()
}

func (sv *Values) setInt64(ctx context.Context, slot slotIdx, newVal int64) {
	if sv.container.setInt64Val(slot, newVal) {
		sv.settingChanged(ctx, slot)
	}
}

// setDefaultOverrideInt64 overrides the default value for the respective
// setting to newVal.
func (sv *Values) setDefaultOverrideInt64(slot slotIdx, newVal int64) {
	sv.overridesMu.Lock()
	defer sv.overridesMu.Unlock()
	sv.overridesMu.defaultOverrides.setInt64Val(slot, newVal)
	sv.setDefaultOverrideLocked(slot)
}

// setDefaultOverrideLocked marks the slot as having an overridden default value.
func (sv *Values) setDefaultOverrideLocked(slot slotIdx) {
	if sv.overridesMu.setOverrides == nil {
		sv.overridesMu.setOverrides = make(map[slotIdx]struct{})
	}
	sv.overridesMu.setOverrides[slot] = struct{}{}
}

// getDefaultOverrides checks whether there's a default override for slotIdx-1.
// If there isn't, the first ret val is false. Otherwise, the first ret val is
// true, the second is the int64 override and the last is a pointer to the
// generic value override. Callers are expected to only use the override value
// corresponding to their setting type.
func (sv *Values) getDefaultOverride(slot slotIdx) (bool, int64, *atomic.Value) {
	sv.overridesMu.Lock()
	defer sv.overridesMu.Unlock()
	if _, ok := sv.overridesMu.setOverrides[slot]; !ok {
		return false, 0, nil
	}
	return true,
		sv.overridesMu.defaultOverrides.intVals[slot],
		&sv.overridesMu.defaultOverrides.genericVals[slot]
}

func (sv *Values) setGeneric(ctx context.Context, slot slotIdx, newVal interface{}) {
	sv.container.setGenericVal(slot, newVal)
	sv.settingChanged(ctx, slot)
}

func (sv *Values) getInt64(slot slotIdx) int64 {
	return sv.container.getInt64(slot)
}

func (sv *Values) getGeneric(slot slotIdx) interface{} {
	return sv.container.getGeneric(slot)
}

// setOnChange installs a callback to be called when a setting's value changes.
// `fn` should avoid doing long-running or blocking work as it is called on the
// goroutine which handles all settings updates.
func (sv *Values) setOnChange(slot slotIdx, fn func(ctx context.Context)) {
	sv.changeMu.Lock()
	sv.changeMu.onChange[slot] = append(sv.changeMu.onChange[slot], fn)
	sv.changeMu.Unlock()
}
