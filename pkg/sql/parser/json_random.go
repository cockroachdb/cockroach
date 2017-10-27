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

package parser

import (
	"encoding/json"
	"fmt"
	"math/rand"
)

func randomDJSON(complexity int) DJSON {
	j, _ := interpretJSON(doRandomDJSON(complexity))
	return j
}

func randomDJSONString() interface{} {
	result := make([]byte, 0)
	l := rand.Intn(10) + 3
	for i := 0; i < l; i++ {
		result = append(result, byte(rand.Intn(0x7f-0x20)+0x20))
	}
	return string(result)
}

func randomDJSONNumber() interface{} {
	return json.Number(fmt.Sprintf("%f", rand.ExpFloat64()))
}

func doRandomDJSON(complexity int) interface{} {
	if complexity <= 0 || rand.Intn(10) == 0 {
		switch rand.Intn(5) {
		case 0:
			return randomDJSONString()
		case 1:
			return randomDJSONNumber()
		case 2:
			return true
		case 3:
			return false
		case 4:
			return nil
		}
	}
	complexity--
	if rand.Intn(2) == 0 {
		result := make([]interface{}, 0)
		for complexity > 0 {
			amount := 1 + rand.Intn(complexity)
			complexity -= amount
			result = append(result, doRandomDJSON(amount))
		}
		return result
	}
	result := make(map[string]interface{})
	for complexity > 0 {
		amount := 1 + rand.Intn(complexity)
		complexity -= amount
		result[randomDJSONString().(string)] = doRandomDJSON(amount)
	}
	return result
}
