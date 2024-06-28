// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lint

import "regexp"

// Various copyright file headers we lint on.
var (
	bslHeader = regexp.MustCompile(`// Copyright 20\d\d The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
`)
	cclHeader = regexp.MustCompile(`// Copyright 20\d\d The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License \(the "License"\); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt
`)

	apacheHeader = regexp.MustCompile(`// Copyright 20\d\d The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 \(the "License"\);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
`)
	// etdApacheHeader is the header of pkg/raft at the time it was imported.
	etcdApacheHeader = regexp.MustCompile(`// Copyright 20\d\d The etcd Authors
//
// Licensed under the Apache License, Version 2.0 \(the "License"\);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
`)
	// cockroachModifiedCopyright is a header that's required to be added any
	// time a file with etdApacheHeader is modified by authors from CRL.
	cockroachModifiedCopyright = regexp.MustCompile(
		`// This code has been modified from its original form by Cockroach Labs, Inc.
// All modifications are Copyright 20\d\d Cockroach Labs, Inc.`)
)
