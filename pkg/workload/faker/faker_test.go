// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package faker

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFaker(t *testing.T) {
	rng := rand.New(rand.NewPCG(0, 0))
	f := NewFaker()

	names := []string{
		"Jane Castillo",
		"Stephen Rhodes",
		"Nicholas Fox DDS",
	}
	for i, name := range names {
		assert.Equal(t, name, f.Name(rng), "testcase %d", i)
	}

	streetAddresses := []string{
		"74921 James Crest",
		"15258 Victoria Point",
		"29930 Joseph Lake Apt. 21",
	}
	for i, streetAddress := range streetAddresses {
		assert.Equal(t, streetAddress, f.StreetAddress(rng), "testcase %d", i)
	}

	wordsets := [][]string{
		{"central"},
		{"paper", "network"},
		{"memory", "call", "summer"},
	}
	for i, words := range wordsets {
		assert.Equal(t, words, f.Words(rng, i+1), "testcase %d", i)
	}

	sentences := [][]string{
		{
			"Investment they whose forget this treatment watch.",
		},
		{
			"Sit type account information fund cut tough.",
			"Wind inside film small such agreement.",
		},
		{
			"Ball build future class nearly generation son pressure.",
			"Out American memory job energy car fast.",
			"President shoulder where already civil miss.",
		},
	}
	for i, sentence := range sentences {
		assert.Equal(t, sentence, f.Sentences(rng, i+1), "testcase %d", i)
	}

	paragraphs := []string{
		"Blue recent community she but education resource picture. Join ok blood Republican never. There ok degree police up.",
		"Perhaps least business bill treat any.",
		"Experience wish day American forget explain since. Discussion meet these other. Might as collection issue movie. Specific interesting get picture show central our.",
	}
	for i, paragraph := range paragraphs {
		assert.Equal(t, paragraph, f.Paragraph(rng), "testcase %d", i)
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
