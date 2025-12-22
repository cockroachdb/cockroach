// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package health

import "net/http"

// HealthDTO is the DTO for the health endpoint.
type HealthDTO struct {
	Data string `json:"data"`
}

// GetData returns the data.
func (dto *HealthDTO) GetData() any {
	return "pong"
}

// GetPublicError always returns nil.
func (dto *HealthDTO) GetError() error {
	return nil
}

func (dto *HealthDTO) GetAssociatedStatusCode() int {
	return http.StatusOK
}

// FileInfo contains information about a single file.
type FileInfo struct {
	Path         string `json:"path"`
	SizeBytes    int64  `json:"size_bytes"`
	ModifiedTime string `json:"modified_time"` // ISO 8601 format
	IsDir        bool   `json:"is_dir"`
}

// DiskUsageDTO contains disk usage information for a directory.
type DiskUsageDTO struct {
	Path        string      `json:"path"`
	TotalBytes  uint64      `json:"total_bytes"`
	UsedBytes   uint64      `json:"used_bytes"`
	AvailBytes  uint64      `json:"avail_bytes"`
	UsedPercent float64     `json:"used_percent"`
	FileCount   int         `json:"file_count"`
	Files       []*FileInfo `json:"files,omitempty"`
	Error       string      `json:"error,omitempty"`
}

// TempDirsDTO contains diagnostics for temporary directories.
type TempDirsDTO struct {
	Directories []*DiskUsageDTO `json:"directories"`
}

func (dto *TempDirsDTO) GetData() any {
	return dto
}

func (dto *TempDirsDTO) GetError() error {
	return nil
}

func (dto *TempDirsDTO) GetAssociatedStatusCode() int {
	return http.StatusOK
}
