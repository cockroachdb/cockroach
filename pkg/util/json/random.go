// Copyright 2017 The Cockroach Authors.
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

package json

import (
	"encoding/json"
	"fmt"
	"math/rand"
)

// Some issues will only be revealed if we have duplicate strings, so we
// include a pool of common strings that we occasionally pull from rather than
// generating a completely random string.
var staticStrings = []string{
	"a",
	"b",
	"c",
	"foo",
	"bar",
	"baz",
	"foobar",
}

// Random generates a random JSON value.
func Random(complexity int, rng *rand.Rand) (JSON, error) {
	return MakeJSON(doRandomJSON(complexity, rng))
}

func randomJSONString(rng *rand.Rand) interface{} {
	if rng.Intn(2) == 0 {
		return staticStrings[rng.Intn(len(staticStrings))]
	}
	result := make([]byte, 0)
	l := rng.Intn(10) + 3
	for i := 0; i < l; i++ {
		result = append(result, byte(rng.Intn(0x7f-0x20)+0x20))
	}
	return string(result)
}

func randomJSONNumber(rng *rand.Rand) interface{} {
	return json.Number(fmt.Sprintf("%v", rand.ExpFloat64()))
}

func doRandomJSON(complexity int, rng *rand.Rand) interface{} {
	if complexity <= 0 || rng.Intn(10) == 0 {
		switch rng.Intn(5) {
		case 0:
			return randomJSONString(rng)
		case 1:
			return randomJSONNumber(rng)
		case 2:
			return true
		case 3:
			return false
		case 4:
			return nil
		}
	}
	complexity--
	switch rng.Intn(3) {
	case 0:
		result := make([]interface{}, 0)
		for complexity > 0 {
			amount := 1 + rng.Intn(complexity)
			complexity -= amount
			result = append(result, doRandomJSON(amount, rng))
		}
		return result
	case 1:
		result := make(map[string]interface{})
		for complexity > 0 {
			amount := 1 + rng.Intn(complexity)
			complexity -= amount
			result[randomJSONString(rng).(string)] = doRandomJSON(amount, rng)
		}
		return result
	default:
		j, _ := Random(complexity, rng)
		encoding, _ := EncodeJSON(nil, j)
		encoded, _ := newEncodedFromRoot(encoding)
		return encoded
	}
}
