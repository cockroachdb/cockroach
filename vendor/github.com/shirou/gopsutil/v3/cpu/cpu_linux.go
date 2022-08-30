//go:build linux
// +build linux

package cpu

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/v3/internal/common"
	"github.com/tklauser/go-sysconf"
)

var ClocksPerSec = float64(100)

func init() {
	clkTck, err := sysconf.Sysconf(sysconf.SC_CLK_TCK)
	// ignore errors
	if err == nil {
		ClocksPerSec = float64(clkTck)
	}
}

func Times(percpu bool) ([]TimesStat, error) {
	return TimesWithContext(context.Background(), percpu)
}

func TimesWithContext(ctx context.Context, percpu bool) ([]TimesStat, error) {
	filename := common.HostProc("stat")
	lines := []string{}
	if percpu {
		statlines, err := common.ReadLines(filename)
		if err != nil || len(statlines) < 2 {
			return []TimesStat{}, nil
		}
		for _, line := range statlines[1:] {
			if !strings.HasPrefix(line, "cpu") {
				break
			}
			lines = append(lines, line)
		}
	} else {
		lines, _ = common.ReadLinesOffsetN(filename, 0, 1)
	}

	ret := make([]TimesStat, 0, len(lines))

	for _, line := range lines {
		ct, err := parseStatLine(line)
		if err != nil {
			continue
		}
		ret = append(ret, *ct)

	}
	return ret, nil
}

func sysCPUPath(cpu int32, relPath string) string {
	return common.HostSys(fmt.Sprintf("devices/system/cpu/cpu%d", cpu), relPath)
}

func finishCPUInfo(c *InfoStat) {
	var lines []string
	var err error
	var value float64

	if len(c.CoreID) == 0 {
		lines, err = common.ReadLines(sysCPUPath(c.CPU, "topology/core_id"))
		if err == nil {
			c.CoreID = lines[0]
		}
	}

	// override the value of c.Mhz with cpufreq/cpuinfo_max_freq regardless
	// of the value from /proc/cpuinfo because we want to report the maximum
	// clock-speed of the CPU for c.Mhz, matching the behaviour of Windows
	lines, err = common.ReadLines(sysCPUPath(c.CPU, "cpufreq/cpuinfo_max_freq"))
	// if we encounter errors below such as there are no cpuinfo_max_freq file,
	// we just ignore. so let Mhz is 0.
	if err != nil || len(lines) == 0 {
		return
	}
	value, err = strconv.ParseFloat(lines[0], 64)
	if err != nil {
		return
	}
	c.Mhz = value / 1000.0 // value is in kHz
	if c.Mhz > 9999 {
		c.Mhz = c.Mhz / 1000.0 // value in Hz
	}
}

// CPUInfo on linux will return 1 item per physical thread.
//
// CPUs have three levels of counting: sockets, cores, threads.
// Cores with HyperThreading count as having 2 threads per core.
// Sockets often come with many physical CPU cores.
// For example a single socket board with two cores each with HT will
// return 4 CPUInfoStat structs on Linux and the "Cores" field set to 1.
func Info() ([]InfoStat, error) {
	return InfoWithContext(context.Background())
}

func InfoWithContext(ctx context.Context) ([]InfoStat, error) {
	filename := common.HostProc("cpuinfo")
	lines, _ := common.ReadLines(filename)

	var ret []InfoStat
	var processorName string

	c := InfoStat{CPU: -1, Cores: 1}
	for _, line := range lines {
		fields := strings.Split(line, ":")
		if len(fields) < 2 {
			continue
		}
		key := strings.TrimSpace(fields[0])
		value := strings.TrimSpace(fields[1])

		switch key {
		case "Processor":
			processorName = value
		case "processor":
			if c.CPU >= 0 {
				finishCPUInfo(&c)
				ret = append(ret, c)
			}
			c = InfoStat{Cores: 1, ModelName: processorName}
			t, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return ret, err
			}
			c.CPU = int32(t)
		case "vendorId", "vendor_id":
			c.VendorID = value
		case "CPU implementer":
			if v, err := strconv.ParseUint(value, 0, 8); err == nil {
				switch v {
				case 0x41:
					c.VendorID = "ARM"
				case 0x42:
					c.VendorID = "Broadcom"
				case 0x43:
					c.VendorID = "Cavium"
				case 0x44:
					c.VendorID = "DEC"
				case 0x46:
					c.VendorID = "Fujitsu"
				case 0x48:
					c.VendorID = "HiSilicon"
				case 0x49:
					c.VendorID = "Infineon"
				case 0x4d:
					c.VendorID = "Motorola/Freescale"
				case 0x4e:
					c.VendorID = "NVIDIA"
				case 0x50:
					c.VendorID = "APM"
				case 0x51:
					c.VendorID = "Qualcomm"
				case 0x56:
					c.VendorID = "Marvell"
				case 0x61:
					c.VendorID = "Apple"
				case 0x69:
					c.VendorID = "Intel"
				case 0xc0:
					c.VendorID = "Ampere"
				}
			}
		case "cpu family":
			c.Family = value
		case "model", "CPU part":
			c.Model = value
		case "model name", "cpu":
			c.ModelName = value
			if strings.Contains(value, "POWER8") ||
				strings.Contains(value, "POWER7") {
				c.Model = strings.Split(value, " ")[0]
				c.Family = "POWER"
				c.VendorID = "IBM"
			}
		case "stepping", "revision", "CPU revision":
			val := value

			if key == "revision" {
				val = strings.Split(value, ".")[0]
			}

			t, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return ret, err
			}
			c.Stepping = int32(t)
		case "cpu MHz", "clock":
			// treat this as the fallback value, thus we ignore error
			if t, err := strconv.ParseFloat(strings.Replace(value, "MHz", "", 1), 64); err == nil {
				c.Mhz = t
			}
		case "cache size":
			t, err := strconv.ParseInt(strings.Replace(value, " KB", "", 1), 10, 64)
			if err != nil {
				return ret, err
			}
			c.CacheSize = int32(t)
		case "physical id":
			c.PhysicalID = value
		case "core id":
			c.CoreID = value
		case "flags", "Features":
			c.Flags = strings.FieldsFunc(value, func(r rune) bool {
				return r == ',' || r == ' '
			})
		case "microcode":
			c.Microcode = value
		}
	}
	if c.CPU >= 0 {
		finishCPUInfo(&c)
		ret = append(ret, c)
	}
	return ret, nil
}

func parseStatLine(line string) (*TimesStat, error) {
	fields := strings.Fields(line)

	if len(fields) == 0 {
		return nil, errors.New("stat does not contain cpu info")
	}

	if !strings.HasPrefix(fields[0], "cpu") {
		return nil, errors.New("not contain cpu")
	}

	cpu := fields[0]
	if cpu == "cpu" {
		cpu = "cpu-total"
	}
	user, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return nil, err
	}
	nice, err := strconv.ParseFloat(fields[2], 64)
	if err != nil {
		return nil, err
	}
	system, err := strconv.ParseFloat(fields[3], 64)
	if err != nil {
		return nil, err
	}
	idle, err := strconv.ParseFloat(fields[4], 64)
	if err != nil {
		return nil, err
	}
	iowait, err := strconv.ParseFloat(fields[5], 64)
	if err != nil {
		return nil, err
	}
	irq, err := strconv.ParseFloat(fields[6], 64)
	if err != nil {
		return nil, err
	}
	softirq, err := strconv.ParseFloat(fields[7], 64)
	if err != nil {
		return nil, err
	}

	ct := &TimesStat{
		CPU:     cpu,
		User:    user / ClocksPerSec,
		Nice:    nice / ClocksPerSec,
		System:  system / ClocksPerSec,
		Idle:    idle / ClocksPerSec,
		Iowait:  iowait / ClocksPerSec,
		Irq:     irq / ClocksPerSec,
		Softirq: softirq / ClocksPerSec,
	}
	if len(fields) > 8 { // Linux >= 2.6.11
		steal, err := strconv.ParseFloat(fields[8], 64)
		if err != nil {
			return nil, err
		}
		ct.Steal = steal / ClocksPerSec
	}
	if len(fields) > 9 { // Linux >= 2.6.24
		guest, err := strconv.ParseFloat(fields[9], 64)
		if err != nil {
			return nil, err
		}
		ct.Guest = guest / ClocksPerSec
	}
	if len(fields) > 10 { // Linux >= 3.2.0
		guestNice, err := strconv.ParseFloat(fields[10], 64)
		if err != nil {
			return nil, err
		}
		ct.GuestNice = guestNice / ClocksPerSec
	}

	return ct, nil
}

func CountsWithContext(ctx context.Context, logical bool) (int, error) {
	if logical {
		ret := 0
		// https://github.com/giampaolo/psutil/blob/d01a9eaa35a8aadf6c519839e987a49d8be2d891/psutil/_pslinux.py#L599
		procCpuinfo := common.HostProc("cpuinfo")
		lines, err := common.ReadLines(procCpuinfo)
		if err == nil {
			for _, line := range lines {
				line = strings.ToLower(line)
				if strings.HasPrefix(line, "processor") {
					_, err = strconv.Atoi(strings.TrimSpace(line[strings.IndexByte(line, ':')+1:]))
					if err == nil {
						ret++
					}
				}
			}
		}
		if ret == 0 {
			procStat := common.HostProc("stat")
			lines, err = common.ReadLines(procStat)
			if err != nil {
				return 0, err
			}
			for _, line := range lines {
				if len(line) >= 4 && strings.HasPrefix(line, "cpu") && '0' <= line[3] && line[3] <= '9' { // `^cpu\d` regexp matching
					ret++
				}
			}
		}
		return ret, nil
	}
	// physical cores
	// https://github.com/giampaolo/psutil/blob/8415355c8badc9c94418b19bdf26e622f06f0cce/psutil/_pslinux.py#L615-L628
	threadSiblingsLists := make(map[string]bool)
	// These 2 files are the same but */core_cpus_list is newer while */thread_siblings_list is deprecated and may disappear in the future.
	// https://www.kernel.org/doc/Documentation/admin-guide/cputopology.rst
	// https://github.com/giampaolo/psutil/pull/1727#issuecomment-707624964
	// https://lkml.org/lkml/2019/2/26/41
	for _, glob := range []string{"devices/system/cpu/cpu[0-9]*/topology/core_cpus_list", "devices/system/cpu/cpu[0-9]*/topology/thread_siblings_list"} {
		if files, err := filepath.Glob(common.HostSys(glob)); err == nil {
			for _, file := range files {
				lines, err := common.ReadLines(file)
				if err != nil || len(lines) != 1 {
					continue
				}
				threadSiblingsLists[lines[0]] = true
			}
			ret := len(threadSiblingsLists)
			if ret != 0 {
				return ret, nil
			}
		}
	}
	// https://github.com/giampaolo/psutil/blob/122174a10b75c9beebe15f6c07dcf3afbe3b120d/psutil/_pslinux.py#L631-L652
	filename := common.HostProc("cpuinfo")
	lines, err := common.ReadLines(filename)
	if err != nil {
		return 0, err
	}
	mapping := make(map[int]int)
	currentInfo := make(map[string]int)
	for _, line := range lines {
		line = strings.ToLower(strings.TrimSpace(line))
		if line == "" {
			// new section
			id, okID := currentInfo["physical id"]
			cores, okCores := currentInfo["cpu cores"]
			if okID && okCores {
				mapping[id] = cores
			}
			currentInfo = make(map[string]int)
			continue
		}
		fields := strings.Split(line, ":")
		if len(fields) < 2 {
			continue
		}
		fields[0] = strings.TrimSpace(fields[0])
		if fields[0] == "physical id" || fields[0] == "cpu cores" {
			val, err := strconv.Atoi(strings.TrimSpace(fields[1]))
			if err != nil {
				continue
			}
			currentInfo[fields[0]] = val
		}
	}
	ret := 0
	for _, v := range mapping {
		ret += v
	}
	return ret, nil
}
