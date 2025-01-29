// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vm

// IArgOverride is an interface for applying overrides to StartupArgs.
type IArgOverride interface {
	apply(args *StartupArgs)
}

// WithVMNameOverride is an override for the VMName field in StartupArgs.
type WithVMNameOverride struct {
	VMName string
}

// apply applies the VMName override to the StartupArgs.
func (o WithVMNameOverride) apply(args *StartupArgs) {
	args.VMName = o.VMName
}

// WithVMName overrides the VMName.
func WithVMName(vmName string) WithVMNameOverride {
	return WithVMNameOverride{VMName: vmName}
}

// WithVMImageOverride is an override for the VMImage field in StartupArgs.
type WithSharedUserOverride struct {
	SharedUser string
}

// apply applies the SharedUser override to the StartupArgs.
func (o WithSharedUserOverride) apply(args *StartupArgs) {
	args.SharedUser = o.SharedUser
}

// WithSharedUser overrides the SharedUser.
func WithSharedUser(sharedUser string) WithSharedUserOverride {
	return WithSharedUserOverride{SharedUser: sharedUser}
}

// WithStartupLogsOverride is an override for the StartupLogs field.
type WithStartupLogsOverride struct {
	StartupLogs string
}

// apply applies the startup logs path override to the StartupArgs.
func (o WithStartupLogsOverride) apply(args *StartupArgs) {
	args.StartupLogs = o.StartupLogs
}

// WithStartupLogs overrides the startup logs path.
func WithStartupLogs(startupLogs string) WithStartupLogsOverride {
	return WithStartupLogsOverride{StartupLogs: startupLogs}
}

// WithOSInitializedFileOverride is an override for the OSInitializedFile field.
type WithOSInitializedFileOverride struct {
	OSInitializedFile string
}

// apply applies the OSInitializedFile override to the StartupArgs.
func (o WithOSInitializedFileOverride) apply(args *StartupArgs) {
	args.OSInitializedFile = o.OSInitializedFile
}

// WithOSInitializedFile overrides the OSInitializedFile.
func WithOSInitializedFile(osInitializedFile string) WithOSInitializedFileOverride {
	return WithOSInitializedFileOverride{OSInitializedFile: osInitializedFile}
}

// WithDisksInitializedFileOverride is an override for the DisksInitializedFile field.
type WithDisksInitializedFileOverride struct {
	DisksInitializedFile string
}

// apply applies the DisksInitializedFile override to the StartupArgs.
func (o WithDisksInitializedFileOverride) apply(args *StartupArgs) {
	args.DisksInitializedFile = o.DisksInitializedFile
}

// WithDisksInitializedFile overrides the DisksInitializedFile.
func WithDisksInitializedFile(disksInitializedFile string) WithDisksInitializedFileOverride {
	return WithDisksInitializedFileOverride{DisksInitializedFile: disksInitializedFile}
}

// WithZfsOverride is an override for the Zfs field.
type WithZfsOverride struct {
	Zfs bool
}

// apply applies the Zfs override to the StartupArgs.
func (o WithZfsOverride) apply(args *StartupArgs) {
	args.Zfs = o.Zfs
}

// WithZfs overrides the Zfs field.
func WithZfs(zfs bool) WithZfsOverride {
	return WithZfsOverride{Zfs: zfs}
}

// WithEnableFIPSOverride is an override for the EnableFIPS field.
type WithEnableFIPSOverride struct {
	EnableFIPS bool
}

// apply applies the EnableFIPS override to the StartupArgs.
func (o WithEnableFIPSOverride) apply(args *StartupArgs) {
	args.EnableFIPS = o.EnableFIPS
}

// WithEnableFIPS overrides the EnableFIPS field.
func WithEnableFIPS(enableFIPS bool) WithEnableFIPSOverride {
	return WithEnableFIPSOverride{EnableFIPS: enableFIPS}
}

// WithEnableCronOverride is an override for the EnableCron field.
type WithEnableCronOverride struct {
	EnableCron bool
}

// apply applies the EnableCron override to the StartupArgs.
func (o WithEnableCronOverride) apply(args *StartupArgs) {
	args.EnableCron = o.EnableCron
}

// WithEnableCron overrides the EnableCron field.
func WithEnableCron(enableCron bool) WithEnableCronOverride {
	return WithEnableCronOverride{EnableCron: enableCron}
}

// WithChronyServersOverride is an override for the ChronyServers field.
type WithChronyServersOverride struct {
	ChronyServers []string
}

// apply applies the ChronyServers override to the StartupArgs.
func (o WithChronyServersOverride) apply(args *StartupArgs) {
	args.ChronyServers = o.ChronyServers
}

// WithChronyServers overrides the ChronyServers field.
func WithChronyServers(chronyServers []string) WithChronyServersOverride {
	return WithChronyServersOverride{ChronyServers: chronyServers}
}

// WithNodeExporterPortOverride is an override for the NodeExporterPort field.
type WithNodeExporterPortOverride struct {
	NodeExporterPort int
}

// apply applies the NodeExporterPort override to the StartupArgs.
func (o WithNodeExporterPortOverride) apply(args *StartupArgs) {
	args.NodeExporterPort = o.NodeExporterPort
}

// WithNodeExporterPort overrides the NodeExporterPort field.
func WithNodeExporterPort(nodeExporterPort int) WithNodeExporterPortOverride {
	return WithNodeExporterPortOverride{NodeExporterPort: nodeExporterPort}
}
