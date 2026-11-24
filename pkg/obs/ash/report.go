// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// totalSampleCount returns the sum of counts across all entries.
func totalSampleCount(entries []AggregatedASH) int64 {
	var total int64
	for i := range entries {
		total += entries[i].Count
	}
	return total
}

// WriteTextReport writes a human-readable aggregated ASH report to w.
// The report is designed for debug zip inspection.
func WriteTextReport(
	w io.Writer, entries []AggregatedASH, generated time.Time, lookback time.Duration,
) error {
	totalSamples := totalSampleCount(entries)

	if _, err := io.WriteString(w, "ASH Aggregated Report\n"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Generated: %s\n", generated.UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Lookback:  %s\n", lookback); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Samples:   %d\n\n", totalSamples); err != nil {
		return err
	}

	if len(entries) == 0 {
		_, err := io.WriteString(w, "No samples in window.\n")
		return err
	}

	// Print header.
	if _, err := fmt.Fprintf(w, "%8s  %-16s  %-22s  %s\n",
		"COUNT", "WORK_EVENT_TYPE", "WORK_EVENT", "WORKLOAD_ID"); err != nil {
		return err
	}

	// Print rows.
	for i := range entries {
		e := &entries[i]
		if _, err := fmt.Fprintf(w, "%8d  %-16s  %-22s  %s\n",
			e.Count, e.WorkEventType, e.WorkEvent, e.WorkloadID); err != nil {
			return err
		}
	}

	return nil
}

// jsonReport is the serialization format for a JSON ASH report.
type jsonReport struct {
	Generated    string      `json:"generated"`
	LookbackSecs float64     `json:"lookback_seconds"`
	TotalSamples int64       `json:"total_samples"`
	Groups       []jsonGroup `json:"groups"`
}

// jsonGroup is a single entry in the JSON report.
type jsonGroup struct {
	WorkEventType string `json:"work_event_type"`
	WorkEvent     string `json:"work_event"`
	WorkloadID    string `json:"workload_id"`
	Count         int64  `json:"count"`
}

// WriteJSONReport writes a machine-readable JSON aggregated ASH report to w.
func WriteJSONReport(
	w io.Writer, entries []AggregatedASH, generated time.Time, lookback time.Duration,
) error {
	totalSamples := totalSampleCount(entries)

	groups := make([]jsonGroup, len(entries))
	for i := range entries {
		groups[i] = jsonGroup{
			WorkEventType: entries[i].WorkEventType.String(),
			WorkEvent:     entries[i].WorkEvent,
			WorkloadID:    entries[i].WorkloadID,
			Count:         entries[i].Count,
		}
	}

	report := jsonReport{
		Generated:    generated.UTC().Format(time.RFC3339Nano),
		LookbackSecs: lookback.Seconds(),
		TotalSamples: totalSamples,
		Groups:       groups,
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}
