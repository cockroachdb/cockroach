// Copyright 2014 The Cockroach Authors.
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

package result

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// IntentsWithArg contains a request and the intents it discovered.
type IntentsWithArg struct {
	Arg     roachpb.Request
	Intents []roachpb.Intent
}

// FromIntents creates a Result communicating that the intents were encountered
// by the given request and should be handled.
func FromIntents(intents []roachpb.Intent, args roachpb.Request, alwaysReturn bool) Result {
	var pd Result
	if len(intents) == 0 {
		return pd
	}
	if alwaysReturn {
		pd.Local.IntentsAlways = &[]IntentsWithArg{{Arg: args, Intents: intents}}
	} else {
		pd.Local.Intents = &[]IntentsWithArg{{Arg: args, Intents: intents}}
	}
	return pd
}
