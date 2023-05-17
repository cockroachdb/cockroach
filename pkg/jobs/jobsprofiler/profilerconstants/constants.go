// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package profilerconstants

import "fmt"

const DSPDiagramInfoKeyPrefix = "~dsp-diag-url-"

// MakeDSPDiagramInfoKey constructs an ephemeral DSP diagram info key.
func MakeDSPDiagramInfoKey(timestampInNanos int64) (string, error) {
	return fmt.Sprintf("%s%d", DSPDiagramInfoKeyPrefix, timestampInNanos), nil
}

// NodeProcessorProgressInfoKeyPrefix is the prefix of the info key used for
// rows that store the per node, per processor progress for a job.
const NodeProcessorProgressInfoKeyPrefix = "~node-processor-progress-"

// MakeNodeProcessorProgressInfoKey returns the info_key used for rows that
// store the per node, per processor progress for a job.
func MakeNodeProcessorProgressInfoKey(
	flowID string, instanceID string, processorID int32,
) (string, error) {
	// The info key is of the form: <prefix>-<flowID>,<instanceID>,<processorID>.
	return fmt.Sprintf("%s%s,%s,%d", NodeProcessorProgressInfoKeyPrefix, flowID, instanceID, processorID), nil
}
