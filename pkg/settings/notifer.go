// Copyright 2023 The Cockroach Authors.
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
	"runtime"
	"runtime/debug"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
)

// NewNotifier is used when a piece of code needs to get a signal when certain
// settings change. It returns a Notifier associated with the given settings
// settings; whenever any of these settings change, a non-blocking send on
// Notifier.Ch() is performed.
//
// The Notifier must be Closed.
func (sv *Values) NewNotifier(settings ...NonMaskedSetting) *Notifier {
	slots := make([]slotIdx, len(settings))
	for i := range settings {
		slots[i] = settings[i].slotIdx()
	}
	ch := make(chan struct{}, 1)
	sv.addOnChangeCh(ch, slots...)
	n := &Notifier{
		sv:    sv,
		slots: slots,
		ch:    ch,
	}
	if buildutil.CrdbTestBuild {
		n.debugStack = string(debug.Stack())
		runtime.SetFinalizer(n, func(obj interface{}) {
			n := obj.(*Notifier)
			if n.sv != nil {
				panic(errors.AssertionFailedf("settings.Notifier not closed; created by\n%s", n.debugStack))
			}
		})
	}
	return n
}

// Notifier is used to listen for changes to a set of settings; see NewNotifier.
type Notifier struct {
	sv         *Values
	slots      []slotIdx
	ch         chan struct{}
	debugStack string
}

// Ch returns a channel that can be listened to for changes to the relevant
// settings.
//
// Note that it is safe to use Ch() while or after calling Close.
func (n *Notifier) Ch() <-chan struct{} {
	return n.ch
}

// Close cleans up the notifier.
func (n *Notifier) Close() {
	if n.sv != nil {
		n.sv.removeOnChangeCh(n.ch, n.slots...)
		n.sv = nil
		n.slots = nil
		// We don't reset n.ch to allow the caller to still have a running goroutine
		// that might try to receive on Ch().
	}
}
