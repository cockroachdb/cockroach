// Package iostat presents I/O and CPU statistics.
package iostat

import "time"

// DriveStats represents I/O statistics of a drive.
type DriveStats struct {
	Name      string // drive name
	Size      int64  // total drive size in bytes
	BlockSize int64  // block size in bytes

	BytesRead      int64
	BytesWritten   int64
	NumRead        int64
	NumWrite       int64
	TotalReadTime  time.Duration
	TotalWriteTime time.Duration
	ReadLatency    time.Duration
	WriteLatency   time.Duration
	ReadErrors     int64
	WriteErrors    int64
	ReadRetries    int64
	WriteRetries   int64
}

// CPUStats represents CPU statistics.
type CPUStats struct {
	// consumed cpu ticks for each.
	User uint64
	Nice uint64
	Sys  uint64
	Idle uint64
}

// LoadAvg represents load averages of the system.
type LoadAvg struct {
	// load averages
	Load1  float64 // over past 1 minute
	Load5  float64 // over past 5 minutes
	Load15 float64 // over past 15 minutes
}
