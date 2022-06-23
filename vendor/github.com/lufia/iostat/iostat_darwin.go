//go:build darwin
// +build darwin

package iostat

// #cgo LDFLAGS: -framework CoreFoundation -framework IOKit
// #include <stdint.h>
// #include <CoreFoundation/CoreFoundation.h>
// #include "iostat_darwin.h"
import "C"
import (
	"fmt"
	"time"
)

// ReadDriveStats returns statistics of each of the drives.
func ReadDriveStats() ([]*DriveStats, error) {
	var buf [C.NDRIVE]C.DriveStats
	_n, err := C.lufia_iostat_v1_readdrivestat(&buf[0], C.int(len(buf)))
	if err != nil {
		return nil, err
	}
	n := int(_n)
	if n < 0 {
		return nil, fmt.Errorf("native function call (`readdrivestat`) failed with an error code: %d", n)
	}
	stats := make([]*DriveStats, n)
	for i := 0; i < n; i++ {
		stats[i] = &DriveStats{
			Name:           C.GoString(&buf[i].name[0]),
			Size:           int64(buf[i].size),
			BlockSize:      int64(buf[i].blocksize),
			BytesRead:      int64(buf[i].read),
			BytesWritten:   int64(buf[i].written),
			NumRead:        int64(buf[i].nread),
			NumWrite:       int64(buf[i].nwrite),
			TotalReadTime:  time.Duration(buf[i].readtime),
			TotalWriteTime: time.Duration(buf[i].writetime),
			ReadLatency:    time.Duration(buf[i].readlat),
			WriteLatency:   time.Duration(buf[i].writelat),
			ReadErrors:     int64(buf[i].readerrs),
			WriteErrors:    int64(buf[i].writeerrs),
			ReadRetries:    int64(buf[i].readretries),
			WriteRetries:   int64(buf[i].writeretries),
		}
	}
	return stats, nil
}

// ReadCPUStats returns statistics of CPU usage.
func ReadCPUStats() (*CPUStats, error) {
	var cpu C.CPUStats
	_, err := C.lufia_iostat_v1_readcpustat(&cpu)
	if err != nil {
		return nil, err
	}
	return &CPUStats{
		User: uint64(cpu.user),
		Nice: uint64(cpu.nice),
		Sys:  uint64(cpu.sys),
		Idle: uint64(cpu.idle),
	}, nil
}

// ReadLoadAvg returns load averages over periods of time.
func ReadLoadAvg() (*LoadAvg, error) {
	var load [3]C.double
	if _, err := C.getloadavg(&load[0], C.int(len(load))); err != nil {
		return nil, err
	}
	return &LoadAvg{
		Load1:  float64(load[0]),
		Load5:  float64(load[1]),
		Load15: float64(load[2]),
	}, nil
}
