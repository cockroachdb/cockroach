// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package flagstub

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
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

// ConfigureProviderFlags implements vm.Provider.
func (p *provider) ConfigureProviderFlags(*pflag.FlagSet, vm.MultipleProjectsOption) {
}

func (p *provider) ConfigureClusterCleanupFlags(*pflag.FlagSet) {

}

func (p *provider) SupportsSpotVMs() bool {
	return false
}

func (p *provider) GetPreemptedSpotVMs(
	l *logger.Logger, vms vm.List, since time.Time,
) ([]vm.PreemptedVM, error) {
	return nil, nil
}

func (p *provider) GetHostErrorVMs(
	l *logger.Logger, vms vm.List, since time.Time,
) ([]string, error) {
	return nil, nil
}

func (p *provider) GetVMSpecs(
	l *logger.Logger, vms vm.List,
) (map[string]map[string]interface{}, error) {
	return nil, nil
}

func (p *provider) CreateVolumeSnapshot(
	l *logger.Logger, volume vm.Volume, vsco vm.VolumeSnapshotCreateOpts,
) (vm.VolumeSnapshot, error) {
	return vm.VolumeSnapshot{}, errors.Newf("%s", p.unimplemented)
}

func (p *provider) ListVolumeSnapshots(
	l *logger.Logger, vslo vm.VolumeSnapshotListOpts,
) ([]vm.VolumeSnapshot, error) {
	return nil, errors.Newf("%s", p.unimplemented)
}

func (p *provider) DeleteVolumeSnapshots(l *logger.Logger, snapshots ...vm.VolumeSnapshot) error {
	return errors.Newf("%s", p.unimplemented)
}

func (p *provider) CreateVolume(*logger.Logger, vm.VolumeCreateOpts) (vm.Volume, error) {
	return vm.Volume{}, errors.Newf("%s", p.unimplemented)
}

func (p *provider) DeleteVolume(l *logger.Logger, volume vm.Volume, vm *vm.VM) error {
	return errors.Newf("%s", p.unimplemented)
}

func (p *provider) ListVolumes(l *logger.Logger, vm *vm.VM) ([]vm.Volume, error) {
	return vm.NonBootAttachedVolumes, nil
}

func (p *provider) AttachVolume(*logger.Logger, vm.Volume, *vm.VM) (string, error) {
	return "", errors.Newf("%s", p.unimplemented)
}

func (p *provider) CreateLoadBalancer(*logger.Logger, vm.List, int) error {
	return nil
}

func (p *provider) DeleteLoadBalancer(*logger.Logger, vm.List, int) error {
	return nil
}

func (p *provider) ListLoadBalancers(*logger.Logger, vm.List) ([]vm.ServiceAddress, error) {
	return nil, nil
}

// CleanSSH implements vm.Provider and is a no-op.
func (p *provider) CleanSSH(l *logger.Logger) error {
	return nil
}

// ConfigSSH implements vm.Provider and is a no-op.
func (p *provider) ConfigSSH(l *logger.Logger, zones []string) error {
	return nil
}

func (p *provider) AddLabels(l *logger.Logger, vms vm.List, labels map[string]string) error {
	return nil
}

func (p *provider) RemoveLabels(l *logger.Logger, vms vm.List, labels []string) error {
	return nil
}

// Create implements vm.Provider and returns Unimplemented.
func (p *provider) Create(
	l *logger.Logger, names []string, opts vm.CreateOpts, providerOpts vm.ProviderOpts,
) (vm.List, error) {
	return nil, errors.Newf("%s", p.unimplemented)
}

// Grow implements vm.Provider and returns Unimplemented.
func (p *provider) Grow(
	l *logger.Logger, vms vm.List, clusterName string, names []string,
) (vm.List, error) {
	return nil, errors.Newf("%s", p.unimplemented)
}

func (p *provider) Shrink(*logger.Logger, vm.List, string) error {
	return errors.Newf("%s", p.unimplemented)
}

// Delete implements vm.Provider and returns Unimplemented.
func (p *provider) Delete(l *logger.Logger, vms vm.List) error {
	return errors.Newf("%s", p.unimplemented)
}

// Reset implements vm.Provider and is a no-op.
func (p *provider) Reset(l *logger.Logger, vms vm.List) error {
	return nil
}

// Extend implements vm.Provider and returns Unimplemented.
func (p *provider) Extend(l *logger.Logger, vms vm.List, lifetime time.Duration) error {
	return errors.Newf("%s", p.unimplemented)
}

// FindActiveAccount implements vm.Provider and returns an empty account.
func (p *provider) FindActiveAccount(l *logger.Logger) (string, error) {
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
