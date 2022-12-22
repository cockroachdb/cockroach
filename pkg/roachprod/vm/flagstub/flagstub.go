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

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
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

func (p *provider) SnapshotVolume(vm.Volume, string, string, map[string]string) (string, error) {
	return "", errors.Newf("%s", p.unimplemented)
}

func (p *provider) CreateVolume(vm.VolumeCreateOpts) (vol vm.Volume, err error) {
	return vol, errors.Newf("%s", p.unimplemented)
}

func (p *provider) AttachVolumeToVM(vm.Volume, *vm.VM) (string, error) {
	return "", errors.Newf("%s", p.unimplemented)
}

// CleanSSH implements vm.Provider and is a no-op.
func (p *provider) CleanSSH() error {
	return nil
}

// ConfigSSH implements vm.Provider and is a no-op.
func (p *provider) ConfigSSH(zones []string) error {
	return nil
}

// Create implements vm.Provider and returns Unimplemented.
func (p *provider) Create(
	l *logger.Logger, names []string, opts vm.CreateOpts, providerOpts vm.ProviderOpts,
) error {
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

// List implements vm.Provider and returns an empty list.
func (p *provider) List(l *logger.Logger, opts vm.ListOptions) (vm.List, error) {
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

// ProjectActive is part of the vm.Provider interface.
func (p *provider) ProjectActive(project string) bool {
	return false
}

// CreateProviderFlags is part of the vm.Provider interface.
func (p *provider) CreateProviderOpts() vm.ProviderOpts {
	return nil
}
