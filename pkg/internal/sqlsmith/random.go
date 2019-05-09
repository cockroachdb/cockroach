// Copyright 2019 The Cockroach Authors.
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

package sqlsmith

func (s *Smither) coin() bool {
	return s.rnd.Intn(2) == 0
}

func (s *Smither) d6() int {
	return s.rnd.Intn(6) + 1
}

func (s *Smither) d9() int {
	return s.rnd.Intn(6) + 1
}

func (s *Smither) d100() int {
	return s.rnd.Intn(100) + 1
}

func (s *scope) coin() bool {
	return s.schema.coin()
}

func (s *scope) d6() int {
	return s.schema.d6()
}

func (s *scope) d9() int {
	return s.schema.d9()
}

func (s *scope) d100() int {
	return s.schema.d100()
}
