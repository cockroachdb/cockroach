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

	"github.com/cockroachdb/errors"
)

// EncodeDuration encodes a duration in the format parseRaw expects.
func EncodeDuration(d time.Duration) string {
	return d.String()
}

// EncodeBool encodes a bool in the format parseRaw expects.
func EncodeBool(b bool) string {
	return strconv.FormatBool(b)
}

// EncodeInt encodes an int in the format parseRaw expects.
func EncodeInt(i int64) string {
	return strconv.FormatInt(i, 10)
}

// EncodeFloat encodes a bool in the format parseRaw expects.
func EncodeFloat(f float64) string {
	return strconv.FormatFloat(f, 'G', -1, 64)
}

type updater struct {
	sv *Values
	m  map[string]struct{}
}

// Updater is a helper for updating the in-memory settings.
//
// RefreshSettings passes the serialized representations of all individual
// settings -- e.g. the rows read from the system.settings table. We update the
// wrapped atomic settings values as we go and note which settings were updated,
// then set the rest to default in ResetRemaining().
type Updater interface {
	Set(ctx context.Context, k, rawValue, valType string) error
	ResetRemaining(ctx context.Context)
}

// A NoopUpdater ignores all updates.
type NoopUpdater struct{}

// Set implements Updater. It is a no-op.
func (u NoopUpdater) Set(ctx context.Context, k, rawValue, valType string) error { return nil }

// ResetRemaining implements Updater. It is a no-op.
func (u NoopUpdater) ResetRemaining(context.Context) {}

// NewUpdater makes an Updater.
func NewUpdater(sv *Values) Updater {
	return updater{
		m:  make(map[string]struct{}, len(registry)),
		sv: sv,
	}
}

// Set attempts to parse and update a setting and notes that it was updated.
func (u updater) Set(ctx context.Context, key, rawValue string, vt string) error {
	d, ok := registry[key]
	if !ok {
		if _, ok := retiredSettings[key]; ok {
			return nil
		}
		// Likely a new setting this old node doesn't know about.
		return errors.Errorf("unknown setting '%s'", key)
	}

	u.m[key] = struct{}{}

	if expected := d.Typ(); vt != expected {
		return errors.Errorf("setting '%s' defined as type %s, not %s", key, expected, vt)
	}

	switch setting := d.(type) {
	case *StringSetting:
		return setting.set(ctx, u.sv, rawValue)
	case *BoolSetting:
		b, err := strconv.ParseBool(rawValue)
		if err != nil {
			return err
		}
		setting.set(ctx, u.sv, b)
		return nil
	case numericSetting: // includes *EnumSetting
		i, err := strconv.Atoi(rawValue)
		if err != nil {
			return err
		}
		return setting.set(ctx, u.sv, int64(i))
	case *FloatSetting:
		f, err := strconv.ParseFloat(rawValue, 64)
		if err != nil {
			return err
		}
		return setting.set(ctx, u.sv, f)
	case *DurationSetting:
		d, err := time.ParseDuration(rawValue)
		if err != nil {
			return err
		}
		return setting.set(ctx, u.sv, d)
	case *DurationSettingWithExplicitUnit:
		d, err := time.ParseDuration(rawValue)
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
		if _, ok := u.m[k]; !ok {
			v.setToDefault(ctx, u.sv)
		}
	}
}
