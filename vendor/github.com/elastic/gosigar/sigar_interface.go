package gosigar

import (
	"time"
)

// ErrNotImplemented is returned when a particular statistic isn't implemented on the host OS.
type ErrNotImplemented struct {
	OS string
}

func (e ErrNotImplemented) Error() string {
	return "not implemented on " + e.OS
}

// IsNotImplemented returns true if the error is ErrNotImplemented
func IsNotImplemented(err error) bool {
	switch err.(type) {
	case ErrNotImplemented, *ErrNotImplemented:
		return true
	default:
		return false
	}
}

// Sigar is an interface for gathering system host stats
type Sigar interface {
	CollectCpuStats(collectionInterval time.Duration) (<-chan Cpu, chan<- struct{})
	GetLoadAverage() (LoadAverage, error)
	GetMem() (Mem, error)
	GetSwap() (Swap, error)
	GetHugeTLBPages(HugeTLBPages, error)
	GetFileSystemUsage(string) (FileSystemUsage, error)
	GetFDUsage() (FDUsage, error)
	GetRusage(who int) (Rusage, error)
}

// Cpu contains CPU time stats
type Cpu struct {
	User    uint64
	Nice    uint64
	Sys     uint64
	Idle    uint64
	Wait    uint64
	Irq     uint64
	SoftIrq uint64
	Stolen  uint64
}

// Total returns total CPU time
func (cpu *Cpu) Total() uint64 {
	return cpu.User + cpu.Nice + cpu.Sys + cpu.Idle +
		cpu.Wait + cpu.Irq + cpu.SoftIrq + cpu.Stolen
}

// Delta returns the difference between two Cpu stat objects
func (cpu Cpu) Delta(other Cpu) Cpu {
	return Cpu{
		User:    cpu.User - other.User,
		Nice:    cpu.Nice - other.Nice,
		Sys:     cpu.Sys - other.Sys,
		Idle:    cpu.Idle - other.Idle,
		Wait:    cpu.Wait - other.Wait,
		Irq:     cpu.Irq - other.Irq,
		SoftIrq: cpu.SoftIrq - other.SoftIrq,
		Stolen:  cpu.Stolen - other.Stolen,
	}
}

// LoadAverage reports standard load averages
type LoadAverage struct {
	One, Five, Fifteen float64
}

// Uptime reports system uptime
type Uptime struct {
	Length float64
}

// Mem contains host memory stats
type Mem struct {
	Total      uint64
	Used       uint64
	Free       uint64
	Cached     uint64
	ActualFree uint64
	ActualUsed uint64
}

// Swap contains stats on swap space
type Swap struct {
	Total uint64
	Used  uint64
	Free  uint64
}

// HugeTLBPages contains HugePages stats
type HugeTLBPages struct {
	Total              uint64
	Free               uint64
	Reserved           uint64
	Surplus            uint64
	DefaultSize        uint64
	TotalAllocatedSize uint64
}

// CpuList contains a list of CPUs on the host system
type CpuList struct {
	List []Cpu
}

// FDUsage contains stats on filesystem usage
type FDUsage struct {
	Open   uint64
	Unused uint64
	Max    uint64
}

// FileSystem contains basic information about a given mounted filesystem
type FileSystem struct {
	DirName     string
	DevName     string
	TypeName    string
	SysTypeName string
	Options     string
	Flags       uint32
}

// FileSystemList gets a list of mounted filesystems
type FileSystemList struct {
	List []FileSystem
}

// FileSystemUsage contains basic stats for the specified filesystem
type FileSystemUsage struct {
	Total     uint64
	Used      uint64
	Free      uint64
	Avail     uint64
	Files     uint64
	FreeFiles uint64
}

// ProcList contains a list of processes found on the host system
type ProcList struct {
	List []int
}

// RunState is a byte-long code used to specify the current runtime state of a process
type RunState byte

const (
	// RunStateSleep corresponds to a sleep state
	RunStateSleep = 'S'
	// RunStateRun corresponds to a running state
	RunStateRun = 'R'
	// RunStateStop corresponds to a stopped state
	RunStateStop = 'T'
	// RunStateZombie marks a zombie process
	RunStateZombie = 'Z'
	// RunStateIdle corresponds to an idle state
	RunStateIdle = 'D'
	// RunStateUnknown corresponds to a process in an unknown state
	RunStateUnknown = '?'
)

// ProcState contains basic metadata and process ownership info for the specified process
type ProcState struct {
	Name      string
	Username  string
	State     RunState
	Ppid      int
	Pgid      int
	Tty       int
	Priority  int
	Nice      int
	Processor int
}

// ProcMem contains memory statistics for a specified process
type ProcMem struct {
	Size        uint64
	Resident    uint64
	Share       uint64
	MinorFaults uint64
	MajorFaults uint64
	PageFaults  uint64
}

// ProcTime contains run time statistics for a specified process
type ProcTime struct {
	StartTime uint64
	User      uint64
	Sys       uint64
	Total     uint64
}

// ProcArgs contains a list of args for a specified process
type ProcArgs struct {
	List []string
}

// ProcEnv contains a map of environment variables for specified process
type ProcEnv struct {
	Vars map[string]string
}

// ProcExe contains basic data about a specified process
type ProcExe struct {
	Name string
	Cwd  string
	Root string
}

// ProcFDUsage contains data on file limits and usage
type ProcFDUsage struct {
	Open      uint64
	SoftLimit uint64
	HardLimit uint64
}

// Rusage contains data on resource usage for a specified process
type Rusage struct {
	Utime    time.Duration
	Stime    time.Duration
	Maxrss   int64
	Ixrss    int64
	Idrss    int64
	Isrss    int64
	Minflt   int64
	Majflt   int64
	Nswap    int64
	Inblock  int64
	Oublock  int64
	Msgsnd   int64
	Msgrcv   int64
	Nsignals int64
	Nvcsw    int64
	Nivcsw   int64
}
