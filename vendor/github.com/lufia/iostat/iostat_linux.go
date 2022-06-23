//go:build !darwin
// +build !darwin

package iostat

import (
	"errors"
)

// ReadDriveStats returns statistics of each of the drives.
func ReadDriveStats() ([]*DriveStats, error) {
	return nil, errors.New("not implement")
}

// ReadCPUStats returns statistics of CPU usage.
func ReadCPUStats() (*CPUStats, error) {
	return nil, errors.New("not implement")
}

// ReadLoadAvg returns load averages over periods of time.
func ReadLoadAvg() (*LoadAvg, error) {
	return nil, errors.New("not implement")
}
