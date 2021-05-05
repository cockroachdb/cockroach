// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package flagstub

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm"
	"github.com/cockroachdb/errors"
)

// New wraps a delegate vm.Provider to only return its name and
// flags.  This allows roachprod to provide a consistent tooling
// experience. Operations that can be reasonably stubbed out are
// implemented as no-op or no-value. All other operations will
// return the provided error.
func New(delegate vm.Provider, unimplemented string) vm.Provider {
	return &provider{delegate: delegate, unimplemented: unimplemented}
}

type provider struct {
	delegate      vm.Provider
	unimplemented string
}

// CleanSSH implements vm.Provider and is a no-op.
func (p *provider) CleanSSH() error {
	return nil
}

// ConfigSSH implements vm.Provider and is a no-op.
func (p *provider) ConfigSSH() error {
	return nil
}

// Create implements vm.Provider and returns Unimplemented.
func (p *provider) Create(names []string, opts vm.CreateOpts) error {
	return errors.Newf("%s", p.unimplemented)
}

// Delete implements vm.Provider and returns Unimplemented.
func (p *provider) Delete(vms vm.List) error {
	return errors.Newf("%s", p.unimplemented)
}

// Reset implements vm.Provider and is a no-op.
func (p *provider) Reset(vms vm.List) error {
	return nil
}

// Extend implements vm.Provider and returns Unimplemented.
func (p *provider) Extend(vms vm.List, lifetime time.Duration) error {
	return errors.Newf("%s", p.unimplemented)
}

// FindActiveAccount implements vm.Provider and returns an empty account.
func (p *provider) FindActiveAccount() (string, error) {
	return "", nil
}

// Flags implements vm.Provider and returns the delegate's name.
func (p *provider) Flags() vm.ProviderFlags {
	return p.delegate.Flags()
}

// List implements vm.Provider and returns an empty list.
func (p *provider) List() (vm.List, error) {
	return nil, nil
}

// Name implements vm.Provider and returns the delegate's name.
func (p *provider) Name() string {
	return p.delegate.Name()
}

// Active is part of the vm.Provider interface.
func (p *provider) Active() bool {
	return false
}
