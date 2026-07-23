// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmpconn

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/jackc/pgtype"
)

func TestCompareVals(t *testing.T) {
	big1, _ := big.NewInt(0).SetString("10986122886681096914", 10)
	big2, _ := big.NewInt(0).SetString("10986122886681097", 10)
	for i, tc := range []struct {
		equal bool
		a, b  []interface{}
	}{
		{
			equal: true,
			a:     []interface{}{apd.New(-2, 0)},
			b:     []interface{}{apd.New(-2, 0)},
		},
		{
			equal: true,
			a:     []interface{}{apd.New(2, 0)},
			b:     []interface{}{apd.New(2, 0)},
		},
		{
			equal: false,
			a:     []interface{}{apd.New(2, 0)},
			b:     []interface{}{apd.New(-2, 0)},
		},
		{
			equal: true,
			a:     []interface{}{apd.New(2, 0)},
			b:     []interface{}{apd.New(2000001, -6)},
		},
		{
			equal: false,
			a:     []interface{}{apd.New(2, 0)},
			b:     []interface{}{apd.New(200001, -5)},
		},
		{
			equal: false,
			a:     []interface{}{apd.New(-2, 0)},
			b:     []interface{}{apd.New(-200001, -5)},
		},
		{
			equal: false,
			a:     []interface{}{apd.New(2, 0)},
			b:     []interface{}{apd.New(-2000001, -6)},
		},
		{
			equal: false,
			a:     []interface{}{apd.New(2, 0)},
			b:     []interface{}{apd.New(2000001, 20)},
		},
		{
			equal: true,
			a: []interface{}{
				pgtype.Numeric{
					Int:    big1,
					Exp:    -19,
					Status: pgtype.Present,
					NaN:    false,
				},
			},
			b: []interface{}{
				pgtype.Numeric{
					Int:    big2,
					Exp:    -16,
					Status: pgtype.Present,
					NaN:    false,
				},
			},
		},
		{
			equal: true,
			a: []interface{}{
				pgtype.Numeric{
					Int:    big.NewInt(0),
					Exp:    -1,
					Status: pgtype.Present,
					NaN:    false,
				},
			},
			b: []interface{}{
				pgtype.Numeric{
					Int:    big.NewInt(0),
					Exp:    6,
					Status: pgtype.Present,
					NaN:    false,
				},
			},
		},
		{
			equal: true,
			a: []interface{}{
				pgtype.Numeric{
					Status: pgtype.Present,
					NaN:    true,
				},
			},
			b: []interface{}{
				pgtype.Numeric{
					Status: pgtype.Present,
					NaN:    true,
				},
			},
		},
	} {

		t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
			err := CompareVals(tc.a, tc.b)
			equal := err == nil
			if equal != tc.equal {
				t.Log("test index", i)
				if err != nil {
					t.Fatal(err)
				} else {
					t.Fatal("expected unequal")
				}
			}
		})
		t.Run(fmt.Sprintf("test_inverted_%d", i), func(t *testing.T) {
			err := CompareVals(tc.b, tc.a)
			equal := err == nil
			if equal != tc.equal {
				t.Log("test index", i)
				if err != nil {
					t.Fatal(err)
				} else {
					t.Fatal("expected unequal")
				}
			}
		})
	}
}
