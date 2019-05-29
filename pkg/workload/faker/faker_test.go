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
		`Tony Johnson`,
		`Jennifer Shaw`,
		`Dr. Andrew Roberts`,
	}
	for i, name := range names {
		assert.Equal(t, name, f.Name(rng), `testcase %d`, i)
	}

	streetAddresses := []string{
		`87890 Coffey Via Apt. 51`,
		`67941 Andrew Station Suite 21`,
		`75031 Samuel Trail`,
	}
	for i, streetAddress := range streetAddresses {
		assert.Equal(t, streetAddress, f.StreetAddress(rng), `testcase %d`, i)
	}

	wordsets := [][]string{
		{`fly`},
		{`until`, `figure`},
		{`after`, `suddenly`, `heavy`},
	}
	for i, words := range wordsets {
		assert.Equal(t, words, f.Words(rng, i+1), `testcase %d`, i)
	}

	sentences := [][]string{
		{
			`Back especially claim rather town human bag.`,
		},
		{
			`Follow play agreement develop.`,
			`Mind deal great national yard various mouth.`,
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
