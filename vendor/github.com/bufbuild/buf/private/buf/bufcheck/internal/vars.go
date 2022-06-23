// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

// priority 1 is higher than priority two
var topLevelCategoryToPriority = map[string]int{
	"MINIMAL":   1,
	"BASIC":     2,
	"DEFAULT":   3,
	"COMMENTS":  4,
	"UNARY_RPC": 5,
	"OTHER":     6,
	"FILE":      1,
	"PACKAGE":   2,
	"WIRE_JSON": 3,
	"WIRE":      4,
}

func categoryCompare(one string, two string) int {
	onePriority, oneIsTopLevel := topLevelCategoryToPriority[one]
	twoPriority, twoIsTopLevel := topLevelCategoryToPriority[two]
	if oneIsTopLevel && !twoIsTopLevel {
		return -1
	}
	if !oneIsTopLevel && twoIsTopLevel {
		return 1
	}
	if oneIsTopLevel && twoIsTopLevel {
		if onePriority < twoPriority {
			return -1
		}
		if onePriority > twoPriority {
			return 1
		}
	}
	if one < two {
		return -1
	}
	if one > two {
		return 1
	}
	return 0
}
