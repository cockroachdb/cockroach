// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settings

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// MaxSettings is the maximum number of settings that the system supports.
// Exported for tests.
const MaxSettings = 1023

// Values is a container that stores values for all registered settings.
// Each setting is assigned a unique slot (up to MaxSettings).
// Note that slot indices are 1-based (this is to trigger panics if an
// uninitialized slot index is used).
type Values struct {
	container valuesContainer

	classCheck classCheck

	defaultOverridesMu struct {
		syncutil.Mutex

		// defaultOverrides maintains the set of overridden default values (see
		// Override()).
		defaultOverrides map[slotIdx]interface{}
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

type classCheck uint32

const (
	// classCheckUndefined is used when the settings.Values hasn't been
	// specialized yet.
	classCheckUndefined classCheck = iota
	// classCheckSystemInterface is used when the settings.Values is
	// specialized for the system interface and SystemOnly settings can
	// be used.
	classCheckSystemInterface
	// classCheckVirtualCluster is used when the settings.Values is
	// specialized for a virtual cluster and SystemOnly settings cannot
	// be used.
	classCheckVirtualCluster
)

func (ck *classCheck) get() classCheck {
	return classCheck(atomic.LoadUint32((*uint32)(ck)))
}

func (ck *classCheck) set(nv classCheck) {
	atomic.StoreUint32((*uint32)(ck), uint32(nv))
}

const numSlots = MaxSettings + 1

type valuesContainer struct {
	intVals     [numSlots]int64
	genericVals [numSlots]atomic.Value

	// If forbidden[slot] is true, that setting is not allowed to be used from the
	// current context (i.e. it is a SystemOnly setting and the container is for a
	// tenant). Reading or writing such a setting causes panics in test builds.
	forbidden [numSlots]bool

	// hasValue contains the origin of the current value of the setting.
	hasValue [numSlots]uint32
}

func (c *valuesContainer) setGenericVal(slot slotIdx, newVal interface{}) {
	if !c.checkForbidden(slot) {
		return
	}
	c.genericVals[slot].Store(newVal)
}

func (c *valuesContainer) setInt64Val(slot slotIdx, newVal int64) (changed bool) {
	if !c.checkForbidden(slot) {
		return false
	}
	return atomic.SwapInt64(&c.intVals[slot], newVal) != newVal
}

func (c *valuesContainer) getInt64(slot slotIdx) int64 {
	c.checkForbidden(slot)
	return atomic.LoadInt64(&c.intVals[slot])
}

func (c *valuesContainer) getGeneric(slot slotIdx) interface{} {
	c.checkForbidden(slot)
	return c.genericVals[slot].Load()
}

// checkForbidden checks if the setting in the given slot is allowed to be used
// from the current context. If not, it panics in test builds and returns false
// in non-test builds.
func (c *valuesContainer) checkForbidden(slot slotIdx) bool {
	if c.forbidden[slot] {
		if buildutil.CrdbTestBuild {
			const msg = `programming error: invalid access to SystemOnly setting %s from a virtual cluster!

TIP: use class ApplicationLevel for settings that configure just 1
virtual cluster; SystemOnly for settings that affect only the shared
storage layer; and SystemVisible for settings that affect the storage
layer and also must be visible to all virtual clusters.
`
			panic(errors.AssertionFailedf(msg, slotTable[slot].Name()))
		}
		return false
	}
	return true
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

const alreadySpecializedError = `programming error: setting value container is already specialized!

TIP: avoid using the same cluster.Settings or settings.Value object across multiple servers.
`

// SpecializeForSystemInterface marks the values container as
// pertaining to the system interface.
func (sv *Values) SpecializeForSystemInterface() {
	if ck := sv.classCheck.get(); ck != classCheckUndefined && ck != classCheckSystemInterface {
		panic(errors.AssertionFailedf(alreadySpecializedError))
	}
	sv.classCheck.set(classCheckSystemInterface)
}

// SpecializeForVirtualCluster marks this container as pertaining to
// a virtual cluster, after which use of SystemOnly values is
// disallowed.
func (sv *Values) SpecializeForVirtualCluster() {
	if ck := sv.classCheck.get(); ck != classCheckUndefined && ck != classCheckVirtualCluster {
		panic(errors.AssertionFailedf(alreadySpecializedError))
	}
	sv.classCheck.set(classCheckVirtualCluster)
	for slot, setting := range slotTable {
		if setting != nil && setting.Class() == SystemOnly {
			sv.container.forbidden[slot] = true
		}
	}
}

// SpecializedToVirtualCluster returns true if this container is for a
// virtual cluster (i.e. SpecializeToVirtualCluster() was called).
func (sv *Values) SpecializedToVirtualCluster() bool {
	return sv.classCheck.get() == classCheckVirtualCluster
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

func (sv *Values) setInt64(ctx context.Context, slot slotIdx, newVal int64) {
	if sv.container.setInt64Val(slot, newVal) {
		sv.settingChanged(ctx, slot)
	}
}

func (sv *Values) setValueOrigin(ctx context.Context, slot slotIdx, origin ValueOrigin) {
	atomic.StoreUint32(&sv.container.hasValue[slot], uint32(origin))
}

func (sv *Values) getValueOrigin(ctx context.Context, slot slotIdx) ValueOrigin {
	return ValueOrigin(atomic.LoadUint32(&sv.container.hasValue[slot]))
}

// setDefaultOverride overrides the default value for the respective setting to
// newVal.
func (sv *Values) setDefaultOverride(slot slotIdx, newVal interface{}) {
	sv.defaultOverridesMu.Lock()
	defer sv.defaultOverridesMu.Unlock()
	if sv.defaultOverridesMu.defaultOverrides == nil {
		sv.defaultOverridesMu.defaultOverrides = make(map[slotIdx]interface{})
	}
	sv.defaultOverridesMu.defaultOverrides[slot] = newVal
}

// getDefaultOverrides checks whether there's a default override for slotIdx-1
// and returns it (or nil if there is no override).
func (sv *Values) getDefaultOverride(slot slotIdx) interface{} {
	sv.defaultOverridesMu.Lock()
	defer sv.defaultOverridesMu.Unlock()
	return sv.defaultOverridesMu.defaultOverrides[slot]
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

// TestingCopyForVirtualCluster makes a copy of the input Values in
// the target Values for use when initializing a server for a virtual
// cluster in tests. This is meant to propagate overrides
// to ApplicationLevel settings.
func (sv *Values) TestingCopyForVirtualCluster(input *Values) {
	for slot := slotIdx(0); slot < slotIdx(len(registry)); slot++ {
		s := slotTable[slot]
		if s.Class() != ApplicationLevel && s.Class() != SystemVisible {
			continue
		}
		// This test-only method is used when creating new virtual clusters which
		// initialize their own version and thus do not want an existing version.
		if s.Typ() == VersionSettingValueType {
			continue
		}

		// Copy the value.
		sv.container.intVals[slot] = atomic.LoadInt64(&input.container.intVals[slot])
		if v := input.container.genericVals[slot].Load(); v != nil {
			sv.container.genericVals[slot].Store(v)
		}

		// Copy the default.
		input.defaultOverridesMu.Lock()
		v, hasVal := input.defaultOverridesMu.defaultOverrides[slot]
		input.defaultOverridesMu.Unlock()
		if !hasVal {
			continue
		}
		sv.setDefaultOverride(slot, v)
	}
}

// TestingCopyForServer makes a copy of the input Values in the target Values
// for use when initializing a server in a test cluster. This is meant to
// propagate initial values and overrides.
func (sv *Values) TestingCopyForServer(input *Values, newOpaque interface{}) {
	sv.opaque = newOpaque
	for slot := slotIdx(0); slot < slotIdx(len(registry)); slot++ {
		// Copy the value.
		sv.container.intVals[slot] = atomic.LoadInt64(&input.container.intVals[slot])
		if v := input.container.genericVals[slot].Load(); v != nil {
			sv.container.genericVals[slot].Store(v)
		}

		// Copy the default.
		input.defaultOverridesMu.Lock()
		v, hasVal := input.defaultOverridesMu.defaultOverrides[slot]
		input.defaultOverridesMu.Unlock()
		if !hasVal {
			continue
		}
		sv.setDefaultOverride(slot, v)
	}
}
