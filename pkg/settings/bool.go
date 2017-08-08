// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package settings

import (
	"sync/atomic"
)

// BoolSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "bool" is updated.
type BoolSetting struct {
	common
	v            int32
	defaultValue bool
}

var _ Setting = &BoolSetting{}

// Get retrieves the bool value in the setting.
func (b *BoolSetting) Get() bool {
	return atomic.LoadInt32(&b.v) != 0
}

func (b *BoolSetting) String() string {
	return EncodeBool(b.Get())
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*BoolSetting) Typ() string {
	return "b"
}

// Override changes the setting without validation.
// For testing usage only.
func (b *BoolSetting) Override(v bool) {
	vInt := int32(0)
	if v {
		vInt = 1
	}
	if atomic.SwapInt32(&b.v, vInt) != vInt {
		b.changed()
	}
}

func (b *BoolSetting) set(v bool) {
	b.Override(v)
}

func (b *BoolSetting) setToDefault() {
	b.set(b.defaultValue)
}

// RegisterBoolSetting defines a new setting with type bool.
func (r *Registry) RegisterBoolSetting(key, desc string, defaultValue bool) *BoolSetting {
	setting := &BoolSetting{defaultValue: defaultValue}
	r.register(key, desc, setting)
	return setting
}

// TestingSetBool returns a mock, unregistered bool setting for testing. It
// takes a pointer to a BoolSetting reference, swapping in the mock setting.
// It returns a cleanup function that swaps back the original setting. This
// function should not be used by tests that run in parallel, as it could
// result in race detector failures, as well as if the cleanup functions are
// called out of order. The original Setting remains registered for
// gossip-driven updates which become visible when it is restored.
func TestingSetBool(s **BoolSetting, v bool) func() {
	saved := *s
	if v {
		*s = &BoolSetting{v: 1}
	} else {
		*s = &BoolSetting{v: 0}
	}
	return func() {
		*s = saved
	}
}

// OnChange registers a callback to be called when the setting changes.
func (b *BoolSetting) OnChange(fn func()) *BoolSetting {
	b.setOnChange(fn)
	return b
}
