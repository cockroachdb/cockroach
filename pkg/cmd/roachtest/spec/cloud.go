// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
