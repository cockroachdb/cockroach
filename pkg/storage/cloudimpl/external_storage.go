// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/*
Package cloudimpl is a stub package that imports all of the concrete
implementations of the various cloud storage providers to trigger their
initialization-time registration with the cloud storage provider registry.
*/
package cloudimpl

import (
	// Import all the cloud provider packages to register them.
	_ "github.com/cockroachdb/cockroach/pkg/storage/cloud/amazon"
	_ "github.com/cockroachdb/cockroach/pkg/storage/cloud/azure"
	_ "github.com/cockroachdb/cockroach/pkg/storage/cloud/gcp"
	_ "github.com/cockroachdb/cockroach/pkg/storage/cloud/httpsink"
	_ "github.com/cockroachdb/cockroach/pkg/storage/cloud/nodelocal"
	_ "github.com/cockroachdb/cockroach/pkg/storage/cloud/nullsink"
	_ "github.com/cockroachdb/cockroach/pkg/storage/cloud/userfile"
)
