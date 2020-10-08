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
<PRE>Specify encryption options for one of the stores on a node. If multiple
stores exist, the flag must be specified for each store.

A valid enterprise license is required to use this functionality.

</PRE>
Key files must be of size 32 bytes + AES key size, such as:
<PRE>
AES-128: 48 bytes
AES-192: 56 bytes
AES-256: 64 bytes

</PRE>
Valid fields:
<PRE>
* path    (required): must match the path of one of the stores
* key     (required): path to the current key file, or "plain"
* old-key (required): path to the previous key file, or "plain"
* rotation-period   : amount of time after which data keys should be rotated

</PRE>
example:
<PRE>
  --enterprise-encryption=path=cockroach-data,key=/keys/aes-128.key,old-key=plain</PRE>
`,
	}
)
