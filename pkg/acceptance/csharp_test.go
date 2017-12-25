// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package acceptance

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestDockerCSharp(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "csharp", []string{"/bin/bash", "-c", strings.Replace(csharp, "%v", "run", 1)})
	testDockerFail(ctx, t, "csharp", []string{"/bin/bash", "-c", strings.Replace(csharp, "%v", "notacommand", 1)})
}

const csharp = `
set -euxo pipefail

cd /mnt/data/csharp

# In dotnet, to get a cert with a private key, we have to use a pfx file.
# See:
# http://www.sparxeng.com/blog/software/x-509-self-signed-certificate-for-cryptography-in-net
# https://stackoverflow.com/questions/808669/convert-a-cert-pem-certificate-to-a-pfx-certificate
openssl pkcs12 -inkey ${PGSSLKEY} -in ${PGSSLCERT} -export -out node.pfx -passout pass:

dotnet %v
`
