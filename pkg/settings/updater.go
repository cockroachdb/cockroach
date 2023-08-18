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
	Set(ctx context.Context, key InternalKey, value EncodedValue) error
	ResetRemaining(ctx context.Context)
	SetValueOrigin(ctx context.Context, key InternalKey, origin ValueOrigin)
}

// A NoopUpdater ignores all updates.
type NoopUpdater struct{}

// Set implements Updater. It is a no-op.
func (u NoopUpdater) Set(ctx context.Context, key InternalKey, value EncodedValue) error { return nil }

// ResetRemaining implements Updater. It is a no-op.
func (u NoopUpdater) ResetRemaining(context.Context) {}

func (u NoopUpdater) SetValueOrigin(ctx context.Context, key InternalKey, origin ValueOrigin) {}

// NewUpdater makes an Updater.
func NewUpdater(sv *Values) Updater {
	if ignoreAllUpdates {
		return NoopUpdater{}
	}
	return updater{
		m:  make(map[InternalKey]struct{}, len(registry)),
		sv: sv,
	}
}

// Set attempts to parse and update a setting and notes that it was updated.
func (u updater) Set(ctx context.Context, key InternalKey, value EncodedValue) error {
	d, ok := registry[key]
	if !ok {
		if _, ok := retiredSettings[key]; ok {
			return nil
		}
		// Likely a new setting this old node doesn't know about.
		return errors.Errorf("unknown setting '%s'", key)
	}

	u.m[key] = struct{}{}

	if expected := d.Typ(); value.Type != expected {
		return errors.Errorf("setting '%s' defined as type %s, not %s", d.Name(), expected, value.Type)
	}

	switch setting := d.(type) {
	case *StringSetting:
		return setting.set(ctx, u.sv, value.Value)
	case *ProtobufSetting:
		p, err := setting.DecodeValue(value.Value)
		if err != nil {
			return err
		}
		return setting.set(ctx, u.sv, p)
	case *BoolSetting:
		b, err := setting.DecodeValue(value.Value)
		if err != nil {
			return err
		}
		setting.set(ctx, u.sv, b)
		return nil
	case numericSetting:
		i, err := setting.DecodeValue(value.Value)
		if err != nil {
			return err
		}
		return setting.set(ctx, u.sv, i)
	case *FloatSetting:
		f, err := setting.DecodeValue(value.Value)
		if err != nil {
			return err
		}
		return setting.set(ctx, u.sv, f)
	case *DurationSetting:
		d, err := setting.DecodeValue(value.Value)
		if err != nil {
			return err
		}
		return setting.set(ctx, u.sv, d)
	case *DurationSettingWithExplicitUnit:
		d, err := setting.DecodeValue(value.Value)
		if err != nil {
			return err
		}
		return setting.set(ctx, u.sv, d)
	case *VersionSetting:
		// We intentionally avoid updating the setting through this code path.
		// The specific setting backed by VersionSetting is the cluster version
		// setting, changes to which are propagated through direct RPCs to each
		// node in the cluster instead of gossip. This is done using the
		// BumpClusterVersion RPC.
		return nil
	}
	return nil
}

// ResetRemaining sets all settings not updated by the updater to their default values.
func (u updater) ResetRemaining(ctx context.Context) {
	for k, v := range registry {

		if _, hasOverride := u.m[k]; hasOverride {
			u.sv.setValueOrigin(ctx, v.getSlot(), OriginExplicitlySet)
		} else {
			u.sv.setValueOrigin(ctx, v.getSlot(), OriginDefault)
		}

		if u.sv.NonSystemTenant() && v.Class() == SystemOnly {
			// Don't try to reset system settings on a non-system tenant.
			continue
		}
		if _, ok := u.m[k]; !ok {
			v.setToDefault(ctx, u.sv)
		}
	}
}

// SetValueOrigin sets the origin of the value of a given setting.
func (u updater) SetValueOrigin(ctx context.Context, key InternalKey, origin ValueOrigin) {
	d, ok := registry[key]
	if ok {
		u.sv.setValueOrigin(ctx, d.getSlot(), origin)
	}
}
