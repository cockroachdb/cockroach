// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
Package impl is a stub package that imports all of the concrete
implementations of the various cloud storage providers to trigger their
initialization-time registration with the cloud storage provider registry.
*/
package impl

import (
	// Import all the cloud provider packages to register them.
	_ "github.com/cockroachdb/cockroach/pkg/cloud/amazon"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/azure"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/gcp"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/httpsink"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/nullsink"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/userfile"
)
