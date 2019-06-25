// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package faker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
)

func TestFaker(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	f := NewFaker()

	names := []string{
		`Daniel Nixon`,
		`Whitney Jimenez`,
		`Brandon Carr`,
	}
	for i, name := range names {
		assert.Equal(t, name, f.Name(rng), `testcase %d`, i)
	}

	streetAddresses := []string{
		`8339 Gary Burgs Apt. 6`,
		`67941 Lawrence Station Suite 29`,
		`29657 Ware Haven`,
	}
	for i, streetAddress := range streetAddresses {
		assert.Equal(t, streetAddress, f.StreetAddress(rng), `testcase %d`, i)
	}

	wordsets := [][]string{
		{`until`},
		{`figure`, `after`},
		{`suddenly`, `heavy`, `time`},
	}
	for i, words := range wordsets {
		assert.Equal(t, words, f.Words(rng, i+1), `testcase %d`, i)
	}

	sentences := [][]string{
		{
			`Especially claim rather town.`,
		},
		{
			`Bag other follow play agreement develop sing.`,
			`Deal great national yard various mouth.`,
		},
		{
			`During talk direction set clear direction.`,
			`Realize once thus administration.`,
			`Glass industry drop prove large age any.`,
		},
	}
	for i, sentence := range sentences {
		assert.Equal(t, sentence, f.Sentences(rng, i+1), `testcase %d`, i)
	}

	paragraphs := []string{
		`Natural purpose member institution picture address. Goal use produce drive worry process ` +
			`beautiful somebody.`,
		`Stuff home capital international. Consumer message bed story here. Contain real expert ` +
			`institution. Against ever seek become put respond. Maybe recently entire history always ` +
			`former.`,
		`Court three author ground. College walk inside coach system career newspaper. However ` +
			`health me community mission. Senior evidence form size true general compare. Teacher look ` +
			`left else.`,
	}
	for i, paragraph := range paragraphs {
		assert.Equal(t, paragraph, f.Paragraph(rng), `testcase %d`, i)
	}
}

func TestFirstToUpper(t *testing.T) {
	tests := []struct {
		input, expected string
	}{
		{`foobar`, `Foobar`},
		{`fooBar`, `FooBar`},
		{`foo bar`, `Foo bar`},
		{`foo Bar`, `Foo Bar`},
		{`κόσμε`, `Κόσμε`},
	}
	for i, test := range tests {
		assert.Equal(t, test.expected, firstToUpper(test.input), `testcase %d`, i)
	}
}
