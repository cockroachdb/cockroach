// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spec

const (
	// AWS stands for Amazon Web Services.
	AWS = "aws"
	// GCE stands for Google Compute Engine.
	GCE = "gce"
	// Azure is Microsoft's cloud.
	Azure = "azure"
	// Local is a faux cloud value assigned to tests that
	// are run on a local machine.
	Local = "local"
)
