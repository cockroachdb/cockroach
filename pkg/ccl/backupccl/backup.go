// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import "github.com/cockroachdb/redact"

// SafeFormat implements redact.SafeFormatter.
func (s *BackupProcessorPlanningTraceEvent) SafeFormat(_ redact.SafePrinter, _ rune) {
	// Needs implementation
}

// SafeFormat implements redact.SafeFormatter.
func (s *BackupProgressTraceEvent) SafeFormat(_ redact.SafePrinter, _ rune) {
	// Needs implementation
}

// SafeFormat implements redact.SafeFormatter.
func (s *BackupExportTraceRequestEvent) SafeFormat(_ redact.SafePrinter, _ rune) {
	// Needs implementation
}

// SafeFormat implements redact.SafeFormatter.
func (s *BackupExportTraceResponseEvent) SafeFormat(_ redact.SafePrinter, _ rune) {
	// Needs implementation
}
