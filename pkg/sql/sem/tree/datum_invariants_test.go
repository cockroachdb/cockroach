// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tree

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

func TestAllTypesCastableToString(t *testing.T) {
	for _, typ := range types.Scalar {
		if ok, _ := isCastDeepValid(typ, types.String); !ok {
			t.Errorf("%s is not castable to STRING, all types should be", typ)
		}
	}
}

func TestAllTypesCastableFromString(t *testing.T) {
	for _, typ := range types.Scalar {
		if ok, _ := isCastDeepValid(types.String, typ); !ok {
			t.Errorf("%s is not castable from STRING, all types should be", typ)
		}
	}
}
