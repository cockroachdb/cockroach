// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliflagsccl

import "github.com/cockroachdb/cockroach/pkg/cli/cliflags"

// Attrs and others store the static information for CLI flags.
var (
	EnterpriseEncryption = cliflags.FlagInfo{
		Name: "enterprise-encryption",
		Description: `
*** Valid enterprise licenses only ***

TODO(mberhault): fill this.

Valid fields:
* path: must match the path of one of the stores
* key: path to the current key file
* old-key: path to the previous key file
* rotation-period: amount of time after which data keys should be rotated
`,
	}
)
