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

import (
	"fmt"

	"github.com/cockroachdb/errors"
)

const DSPDiagramInfoKeyPrefix = "~dsp-diag-url-"

// MakeDSPDiagramInfoKey constructs an ephemeral DSP diagram info key.
func MakeDSPDiagramInfoKey(parts ...any) (string, error) {
	if len(parts) != 1 {
		return "", errors.AssertionFailedf("expected 1 part got %d", len(parts))
	}
	return fmt.Sprintf("%s%d", DSPDiagramInfoKeyPrefix, parts[0]), nil
}

// NodeProcessorProgressInfoKeyPrefix is the prefix of the info key used for
// rows that store the per node, per processor progress for a job.
const NodeProcessorProgressInfoKeyPrefix = "~node-processor-progress-"

// MakeNodeProcessorProgressInfoKey returns the info_key used for rows that
// store the per node, per processor progress for a job.
func MakeNodeProcessorProgressInfoKey(parts ...any) (string, error) {
	if len(parts) != 3 {
		return "", errors.AssertionFailedf("expected 3 parts got %d", len(parts))
	}
	// The info key is of the form: <prefix>-<flowID>,<instanceID>,<processorID>.
	return fmt.Sprintf("%s%s,%s,%d", NodeProcessorProgressInfoKeyPrefix, parts[0], parts[1], parts[2]), nil
}
