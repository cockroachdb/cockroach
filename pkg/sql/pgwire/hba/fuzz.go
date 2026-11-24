// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build gofuzz

package hba

import (
	"fmt"

	"github.com/kr/pretty"
)

func FuzzParseAndNormalize(data []byte) int {
	conf, err := ParseAndNormalize(string(data))
	if err != nil {
		return 0
	}
	s := conf.String()
	conf2, err := ParseAndNormalize(s)
	if err != nil {
		panic(fmt.Errorf(`-- original:
%s
-- parsed:
%# v
-- new:
%s
-- error:
%v`,
			string(data),
			pretty.Formatter(conf),
			s,
			err))
	}
	s2 := conf2.String()
	if s != s2 {
		panic(fmt.Errorf(`reparse mismatch:
-- original:
%s
-- new:
%# v
-- printed:
%s`,
			s,
			pretty.Formatter(conf2),
			s2))
	}
	return 1
}
