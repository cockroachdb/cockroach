// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sqlsmith

func (s *Smither) coin() bool {
	return s.rnd.Intn(2) == 0
}

func (s *Smither) d6() int {
	return s.rnd.Intn(6) + 1
}

func (s *Smither) d9() int {
	return s.rnd.Intn(9) + 1
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
