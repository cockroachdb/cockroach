// Copyright (c) 2012 VMware, Inc.

package gosigar

/*
#include <stdlib.h>
#include <sys/sysctl.h>
#include <sys/mount.h>
#include <mach/mach_init.h>
#include <mach/mach_host.h>
#include <mach/host_info.h>
#include <libproc.h>
#include <mach/processor_info.h>
#include <mach/vm_map.h>
*/
import "C"

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os/user"
	"runtime"
	"strconv"
	"syscall"
	"time"
	"unsafe"
)

// Get fetches LoadAverage data
func (self *LoadAverage) Get() error {
	avg := []C.double{0, 0, 0}

	C.getloadavg(&avg[0], C.int(len(avg)))

	self.One = float64(avg[0])
	self.Five = float64(avg[1])
	self.Fifteen = float64(avg[2])

	return nil
}

// Get fetches memory data
func (self *Mem) Get() error {
	var vmstat C.vm_statistics_data_t

	if err := sysctlbyname("hw.memsize", &self.Total); err != nil {
		return err
	}

	if err := vmInfo(&vmstat); err != nil {
		return err
	}

	kern := uint64(vmstat.inactive_count) << 12
	self.Free = uint64(vmstat.free_count) << 12

	self.Used = self.Total - self.Free
	self.ActualFree = self.Free + kern
	self.ActualUsed = self.Used - kern

	return nil
}

type xswUsage struct {
	Total, Avail, Used uint64
}

// Get fetches swap data
func (self *Swap) Get() error {
	swUsage := xswUsage{}

	if err := sysctlbyname("vm.swapusage", &swUsage); err != nil {
		return err
	}

	self.Total = swUsage.Total
	self.Used = swUsage.Used
	self.Free = swUsage.Avail

	return nil
}

// Get fetches hugepages data
func (self *HugeTLBPages) Get() error {
	return ErrNotImplemented{runtime.GOOS}
}

// Get fetches CPU data
func (self *Cpu) Get() error {
	var count C.mach_msg_type_number_t = C.HOST_CPU_LOAD_INFO_COUNT
	var cpuload C.host_cpu_load_info_data_t

	status := C.host_statistics(C.host_t(C.mach_host_self()),
		C.HOST_CPU_LOAD_INFO,
		C.host_info_t(unsafe.Pointer(&cpuload)),
		&count)

	if status != C.KERN_SUCCESS {
		return fmt.Errorf("host_statistics error=%d", status)
	}

	self.User = uint64(cpuload.cpu_ticks[C.CPU_STATE_USER])
	self.Sys = uint64(cpuload.cpu_ticks[C.CPU_STATE_SYSTEM])
	self.Idle = uint64(cpuload.cpu_ticks[C.CPU_STATE_IDLE])
	self.Nice = uint64(cpuload.cpu_ticks[C.CPU_STATE_NICE])

	return nil
}

// Get fetches CPU list data
func (self *CpuList) Get() error {
	var count C.mach_msg_type_number_t
	var cpuload *C.processor_cpu_load_info_data_t
	var ncpu C.natural_t

	status := C.host_processor_info(C.host_t(C.mach_host_self()),
		C.PROCESSOR_CPU_LOAD_INFO,
		&ncpu,
		(*C.processor_info_array_t)(unsafe.Pointer(&cpuload)),
		&count)

	if status != C.KERN_SUCCESS {
		return fmt.Errorf("host_processor_info error=%d", status)
	}

	// jump through some cgo casting hoops and ensure we properly free
	// the memory that cpuload points to
	target := C.vm_map_t(C.mach_task_self_)
	address := C.vm_address_t(uintptr(unsafe.Pointer(cpuload)))
	defer C.vm_deallocate(target, address, C.vm_size_t(ncpu))

	// the body of struct processor_cpu_load_info
	// aka processor_cpu_load_info_data_t
	var cpuTicks [C.CPU_STATE_MAX]uint32

	// copy the cpuload array to a []byte buffer
	// where we can binary.Read the data
	size := int(ncpu) * binary.Size(cpuTicks)
	buf := C.GoBytes(unsafe.Pointer(cpuload), C.int(size))

	bbuf := bytes.NewBuffer(buf)

	self.List = make([]Cpu, 0, ncpu)

	for i := 0; i < int(ncpu); i++ {
		cpu := Cpu{}

		err := binary.Read(bbuf, binary.LittleEndian, &cpuTicks)
		if err != nil {
			return err
		}

		cpu.User = uint64(cpuTicks[C.CPU_STATE_USER])
		cpu.Sys = uint64(cpuTicks[C.CPU_STATE_SYSTEM])
		cpu.Idle = uint64(cpuTicks[C.CPU_STATE_IDLE])
		cpu.Nice = uint64(cpuTicks[C.CPU_STATE_NICE])

		self.List = append(self.List, cpu)
	}

	return nil
}

// Get returns FD usage data
func (self *FDUsage) Get() error {
	return ErrNotImplemented{runtime.GOOS}
}

// Get returns filesystem data
func (self *FileSystemList) Get() error {
	num, err := syscall.Getfsstat(nil, C.MNT_NOWAIT)
	if err != nil {
		return err
	}

	buf := make([]syscall.Statfs_t, num)

	_, err = syscall.Getfsstat(buf, C.MNT_NOWAIT)
	if err != nil {
		return err
	}

	fslist := make([]FileSystem, 0, num)

	for i := 0; i < num; i++ {
		fs := FileSystem{}
		fs.DirName = byteListToString(buf[i].Mntonname[:])
		fs.DevName = byteListToString(buf[i].Mntfromname[:])
		fs.SysTypeName = byteListToString(buf[i].Fstypename[:])

		fslist = append(fslist, fs)
	}

	self.List = fslist

	return err
}

// Get returns a process list
func (self *ProcList) Get() error {
	n := C.proc_listpids(C.PROC_ALL_PIDS, 0, nil, 0)
	if n <= 0 {
		return syscall.EINVAL
	}
	buf := make([]byte, n)
	n = C.proc_listpids(C.PROC_ALL_PIDS, 0, unsafe.Pointer(&buf[0]), n)
	if n <= 0 {
		return syscall.ENOMEM
	}

	var pid int32
	num := int(n) / binary.Size(pid)
	list := make([]int, 0, num)
	bbuf := bytes.NewBuffer(buf)

	for i := 0; i < num; i++ {
		if err := binary.Read(bbuf, binary.LittleEndian, &pid); err != nil {
			return err
		}
		if pid == 0 {
			continue
		}

		list = append(list, int(pid))
	}

	self.List = list

	return nil
}

// Get returns process state data
func (self *ProcState) Get(pid int) error {
	info := C.struct_proc_taskallinfo{}

	if err := taskInfo(pid, &info); err != nil {
		return err
	}

	self.Name = C.GoString(&info.pbsd.pbi_comm[0])

	switch info.pbsd.pbi_status {
	case C.SIDL:
		self.State = RunStateIdle
	case C.SRUN:
		self.State = RunStateRun
	case C.SSLEEP:
		self.State = RunStateSleep
	case C.SSTOP:
		self.State = RunStateStop
	case C.SZOMB:
		self.State = RunStateZombie
	default:
		self.State = RunStateUnknown
	}

	self.Ppid = int(info.pbsd.pbi_ppid)

	self.Pgid = int(info.pbsd.pbi_pgid)

	self.Tty = int(info.pbsd.e_tdev)

	self.Priority = int(info.ptinfo.pti_priority)

	self.Nice = int(info.pbsd.pbi_nice)

	// Get process username. Fallback to UID if username is not available.
	uid := strconv.Itoa(int(info.pbsd.pbi_uid))
	user, err := user.LookupId(uid)
	if err == nil && user.Username != "" {
		self.Username = user.Username
	} else {
		self.Username = uid
	}

	return nil
}

// Get returns process memory data
func (self *ProcMem) Get(pid int) error {
	info := C.struct_proc_taskallinfo{}

	if err := taskInfo(pid, &info); err != nil {
		return err
	}

	self.Size = uint64(info.ptinfo.pti_virtual_size)
	self.Resident = uint64(info.ptinfo.pti_resident_size)
	self.PageFaults = uint64(info.ptinfo.pti_faults)

	return nil
}

// Get returns process time data
func (self *ProcTime) Get(pid int) error {
	info := C.struct_proc_taskallinfo{}

	if err := taskInfo(pid, &info); err != nil {
		return err
	}

	self.User =
		uint64(info.ptinfo.pti_total_user) / uint64(time.Millisecond)

	self.Sys =
		uint64(info.ptinfo.pti_total_system) / uint64(time.Millisecond)

	self.Total = self.User + self.Sys

	self.StartTime = (uint64(info.pbsd.pbi_start_tvsec) * 1000) +
		(uint64(info.pbsd.pbi_start_tvusec) / 1000)

	return nil
}

// Get returns process arg data
func (self *ProcArgs) Get(pid int) error {
	var args []string

	argv := func(arg string) {
		args = append(args, arg)
	}

	err := kernProcargs(pid, nil, argv, nil)

	self.List = args

	return err
}

// Get returns process environment data
func (self *ProcEnv) Get(pid int) error {
	if self.Vars == nil {
		self.Vars = map[string]string{}
	}

	env := func(k, v string) {
		self.Vars[k] = v
	}

	return kernProcargs(pid, nil, nil, env)
}

// Get returns process exec data
func (self *ProcExe) Get(pid int) error {
	exe := func(arg string) {
		self.Name = arg
	}

	return kernProcargs(pid, exe, nil, nil)
}

// Get returns process file usage
func (self *ProcFDUsage) Get(pid int) error {
	return ErrNotImplemented{runtime.GOOS}
}

// kernProcargs is a wrapper around sysctl KERN_PROCARGS2
// callbacks params are optional,
// up to the caller as to which pieces of data they want
func kernProcargs(pid int,
	exe func(string),
	argv func(string),
	env func(string, string)) error {

	mib := []C.int{C.CTL_KERN, C.KERN_PROCARGS2, C.int(pid)}
	argmax := uintptr(C.ARG_MAX)
	buf := make([]byte, argmax)
	err := sysctl(mib, &buf[0], &argmax, nil, 0)
	if err != nil {
		return nil
	}

	bbuf := bytes.NewBuffer(buf)
	bbuf.Truncate(int(argmax))

	var argc int32
	binary.Read(bbuf, binary.LittleEndian, &argc)

	path, err := bbuf.ReadBytes(0)
	if err != nil {
		return fmt.Errorf("Error reading the argv[0]: %v", err)
	}
	if exe != nil {
		exe(string(chop(path)))
	}

	// skip trailing \0's
	for {
		c, err := bbuf.ReadByte()
		if err != nil {
			return fmt.Errorf("Error skipping nils: %v", err)
		}
		if c != 0 {
			bbuf.UnreadByte()
			break // start of argv[0]
		}
	}

	for i := 0; i < int(argc); i++ {
		arg, err := bbuf.ReadBytes(0)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("Error reading args: %v", err)
		}
		if argv != nil {
			argv(string(chop(arg)))
		}
	}

	if env == nil {
		return nil
	}

	delim := []byte{61} // "="

	for {
		line, err := bbuf.ReadBytes(0)
		if err == io.EOF || line[0] == 0 {
			break
		}
		if err != nil {
			return fmt.Errorf("Error reading args: %v", err)
		}
		pair := bytes.SplitN(chop(line), delim, 2)

		if len(pair) != 2 {
			return fmt.Errorf("Error reading process information for PID: %d", pid)
		}

		env(string(pair[0]), string(pair[1]))
	}

	return nil
}

// XXX copied from zsyscall_darwin_amd64.go
func sysctl(mib []C.int, old *byte, oldlen *uintptr,
	new *byte, newlen uintptr) (err error) {
	var p0 unsafe.Pointer
	p0 = unsafe.Pointer(&mib[0])
	_, _, e1 := syscall.Syscall6(syscall.SYS___SYSCTL, uintptr(p0),
		uintptr(len(mib)),
		uintptr(unsafe.Pointer(old)), uintptr(unsafe.Pointer(oldlen)),
		uintptr(unsafe.Pointer(new)), uintptr(newlen))
	if e1 != 0 {
		err = e1
	}
	return
}

func vmInfo(vmstat *C.vm_statistics_data_t) error {
	var count C.mach_msg_type_number_t = C.HOST_VM_INFO_COUNT

	status := C.host_statistics(
		C.host_t(C.mach_host_self()),
		C.HOST_VM_INFO,
		C.host_info_t(unsafe.Pointer(vmstat)),
		&count)

	if status != C.KERN_SUCCESS {
		return fmt.Errorf("host_statistics=%d", status)
	}

	return nil
}

// generic Sysctl buffer unmarshalling
func sysctlbyname(name string, data interface{}) (err error) {
	val, err := syscall.Sysctl(name)
	if err != nil {
		return err
	}

	buf := []byte(val)

	switch v := data.(type) {
	case *uint64:
		*v = *(*uint64)(unsafe.Pointer(&buf[0]))
		return
	}

	bbuf := bytes.NewBuffer([]byte(val))
	return binary.Read(bbuf, binary.LittleEndian, data)
}

func taskInfo(pid int, info *C.struct_proc_taskallinfo) error {
	size := C.int(unsafe.Sizeof(*info))
	ptr := unsafe.Pointer(info)

	n := C.proc_pidinfo(C.int(pid), C.PROC_PIDTASKALLINFO, 0, ptr, size)
	if n != size {
		return fmt.Errorf("Could not read process info for pid %d", pid)
	}

	return nil
}
