// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settings

import (
	"context"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

var ignoreAllUpdates = envutil.EnvOrDefaultBool("COCKROACH_IGNORE_CLUSTER_SETTINGS", false)

// IsIgnoringAllUpdates returns true if Updaters returned by NewUpdater will
// discard all updates due to the COCKROACH_IGNORE_CLUSTER_SETTINGS var.
func IsIgnoringAllUpdates() bool {
	return ignoreAllUpdates
}

// EncodeDuration encodes a duration in the format of EncodedValue.Value.
func EncodeDuration(d time.Duration) string {
	return d.String()
}

// EncodeBool encodes a bool in the format of EncodedValue.Value.
func EncodeBool(b bool) string {
	return strconv.FormatBool(b)
}

// EncodeInt encodes an int in the format of EncodedValue.Value.
func EncodeInt(i int64) string {
	return strconv.FormatInt(i, 10)
}

// EncodeFloat encodes a float in the format of EncodedValue.Value.
func EncodeFloat(f float64) string {
	return strconv.FormatFloat(f, 'G', -1, 64)
}

// EncodeProtobuf encodes a protobuf in the format of EncodedValue.Value.
func EncodeProtobuf(p protoutil.Message) string {
	data := make([]byte, p.Size())
	if _, err := p.MarshalTo(data); err != nil {
		panic(errors.Wrapf(err, "encoding %s: %+v", proto.MessageName(p), p))
	}
	return string(data)
}

type updater struct {
	sv *Values
	m  map[InternalKey]struct{}
}

// Updater is a helper for updating the in-memory settings.
//
// RefreshSettings passes the serialized representations of all individual
// settings -- e.g. the rows read from the system.settings table. We update the
// wrapped atomic settings values as we go and note which settings were updated,
// then set the rest to default in ResetRemaining().
type Updater interface {
	// Set is used by tests to configure overrides in a generic fashion.
	Set(ctx context.Context, key InternalKey, value EncodedValue) error
	// SetToDefault resets the setting to its default value.
	SetToDefault(ctx context.Context, key InternalKey) error

	// ResetRemaining loads the values from build-time defaults for all
	// settings that were not updated by Set() and SetFromStorage().
	ResetRemaining(ctx context.Context)

	// SetFromStorage is called by the settings watcher to update the
	// settings from either the rangefeed over system.settings, or
	// overrides coming in over the network from the system tenant.
	SetFromStorage(ctx context.Context, key InternalKey, value EncodedValue, origin ValueOrigin) error
}

// A NoopUpdater ignores all updates.
type NoopUpdater struct{}

// Set implements Updater. It is a no-op.
func (u NoopUpdater) Set(ctx context.Context, key InternalKey, value EncodedValue) error { return nil }

// ResetRemaining implements Updater. It is a no-op.
func (u NoopUpdater) ResetRemaining(context.Context) {}

// SetToDefault implements Updater. It is a no-op.
func (u NoopUpdater) SetToDefault(ctx context.Context, key InternalKey) error { return nil }

// SetFromStorage implements Updater. It is a no-op.
func (u NoopUpdater) SetFromStorage(
	ctx context.Context, key InternalKey, value EncodedValue, origin ValueOrigin,
) error {
	return nil
}

// NewUpdater makes an Updater.
func NewUpdater(sv *Values) Updater {
	if ignoreAllUpdates {
		return NoopUpdater{}
	}
	return updater{
		sv: sv,
		m:  make(map[InternalKey]struct{}),
	}
}

// getSetting determines whether the target setting can be set or
// overridden.
func (u updater) getSetting(key InternalKey, value EncodedValue) (internalSetting, error) {
	d, ok := registry[key]
	if !ok {
		if _, ok := retiredSettings[key]; ok {
			return nil, nil
		}
		// Likely a new setting this old node doesn't know about.
		return nil, errors.Errorf("unknown setting '%s'", key)
	}
	return d, nil
}

func checkType(d internalSetting, value EncodedValue) error {
	if expected := d.Typ(); value.Type != expected {
		return errors.Errorf("setting '%s' defined as type %s, not %s", d.Name(), expected, value.Type)
	}
	return nil
}

// Set attempts update a setting and notes that it was updated.
func (u updater) Set(ctx context.Context, key InternalKey, value EncodedValue) error {
	d, err := u.getSetting(key, value)
	if err != nil || d == nil {
		return err
	}

	return u.setInternal(ctx, key, value, d, OriginExplicitlySet)
}

// setInternal is the shared code between Set() and SetFromStorage().
// It propagates the given value to the in-RAM store and keeps track
// of the origin of the value.
func (u updater) setInternal(
	ctx context.Context, key InternalKey, value EncodedValue, d internalSetting, origin ValueOrigin,
) error {
	// Mark the setting as modified, such that
	// (updater).ResetRemaining() does not touch it.
	u.m[key] = struct{}{}

	if err := checkType(d, value); err != nil {
		return err
	}
	u.sv.setValueOrigin(ctx, d.getSlot(), origin)
	return d.decodeAndSet(ctx, u.sv, value.Value)
}

func (u updater) SetToDefault(ctx context.Context, key InternalKey) error {
	d, ok := registry[key]
	if !ok {
		if _, ok := retiredSettings[key]; ok {
			return nil
		}
		// Likely a new setting this old node doesn't know about.
		return errors.Errorf("unknown setting '%s'", key)
	}

	u.m[key] = struct{}{}
	u.sv.setValueOrigin(ctx, d.getSlot(), OriginDefault)
	d.setToDefault(ctx, u.sv)
	return nil
}

// ResetRemaining sets all settings not explicitly set via this
// updater to their default values.
func (u updater) ResetRemaining(ctx context.Context) {
	for _, v := range registry {
		if u.sv.SpecializedToVirtualCluster() && v.Class() == SystemOnly {
			// Don't try to reset system settings on a non-system tenant.
			continue
		}
		if _, setInThisUpdater := u.m[v.InternalKey()]; !setInThisUpdater &&
			// We need to preserve test overrides.
			u.sv.getValueOrigin(ctx, v.getSlot()) != OriginOverride {
			u.sv.setValueOrigin(ctx, v.getSlot(), OriginDefault)
			v.setToDefault(ctx, u.sv)
		}
	}
}

// SetFromStorage loads the stored value into the setting.
func (u updater) SetFromStorage(
	ctx context.Context, key InternalKey, value EncodedValue, origin ValueOrigin,
) error {
	d, err := u.getSetting(key, value)
	if err != nil || d == nil {
		return err
	}

	if !u.sv.SpecializedToVirtualCluster() /* system tenant */ ||
		d.Class() == ApplicationLevel {
		// The value is being loaded from the current virtual cluster's
		// system.settings. Load it as an active value.
		return u.setInternal(ctx, key, value, d, origin)
	}

	// Here we are looking at a SystemVisible or SystemOnly setting
	// from within a virtual cluster.

	if d.Class() == SystemOnly {
		// Attempting to load a SystemOnly setting from a virtual
		// cluster's system.settings table.
		//
		// This is always invalid. The caller should have prevented this
		// point from being reached, so this is really protection against
		// API misuse.
		return errors.AssertionFailedf("programming error: cannot set SystemOnly %q", key)
	}

	if d.Class() != SystemVisible {
		return errors.AssertionFailedf("unhandled class %v", d.Class())
	}

	if err := checkType(d, value); err != nil {
		return err
	}

	defer func() {
		// After the code below sets the default value, we ensure that the
		// new default is also copied to the in-RAM store for all settings
		// that didn't have an active value in the virtual cluster yet.
		if u.sv.getValueOrigin(ctx, d.getSlot()) == OriginDefault {
			d.setToDefault(ctx, u.sv)
		}
	}()

	// We are receiving an alternate default for a SystemVisible
	// setting. Here we do not configure the main setting value (via
	// setInternal or .set on the setting itself): many tests use
	// .Override earlier and we do not want to change the override.
	// Instead, we update the default.
	return d.decodeAndSetDefaultOverride(ctx, u.sv, value.Value)
}
