// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package chunker

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/embedding/tokenizer"
	"github.com/stretchr/testify/require"
)

// loadTestTokenizer creates a tokenizer from a minimal vocabulary
// suitable for testing. Each common English word maps to a single
// token, making token counts predictable.
func loadTestTokenizer(t *testing.T) *tokenizer.Tokenizer {
	t.Helper()
	// Build a vocabulary with single-token words for predictable
	// token counts. Each line is one token.
	vocabLines := []string{
		"[PAD]", "[UNK]", "[CLS]", "[SEP]", "[MASK]",
		"the", "cat", "sat", "on", "mat",
		"dog", "ran", "fast", "and", "jumped",
		"a", "is", "was", "big", "small",
		"hello", "world", "this", "that", "with",
		"over", "lazy", "fox", "quick", "brown",
		".", "!", "?", ",",
	}
	vocabText := strings.Join(vocabLines, "\n") + "\n"
	vocab, err := tokenizer.LoadVocab(strings.NewReader(vocabText))
	require.NoError(t, err)
	return tokenizer.NewTokenizer(vocab, tokenizer.WithMaxSeqLen(256))
}

func TestChunkSingleChunk(t *testing.T) {
	tok := loadTestTokenizer(t)
	c := NewChunker(tok, WithMaxTokens(50), WithOverlapTokens(10))

	chunks := c.Chunk("the cat sat on the mat.")
	require.Len(t, chunks, 1)
	require.Equal(t, "the cat sat on the mat.", chunks[0].Text)
	require.Equal(t, 0, chunks[0].SeqNum)
}

func TestChunkEmptyText(t *testing.T) {
	tok := loadTestTokenizer(t)
	c := NewChunker(tok, WithMaxTokens(50), WithOverlapTokens(10))

	chunks := c.Chunk("")
	require.Len(t, chunks, 1)
	require.Equal(t, "", chunks[0].Text)
	require.Equal(t, 0, chunks[0].SeqNum)
}

func TestChunkMultipleChunks(t *testing.T) {
	tok := loadTestTokenizer(t)
	// Set maxTokens very low to force multiple chunks. Each word in
	// our vocabulary is 1 token, so "the cat sat." is ~4 tokens.
	c := NewChunker(tok, WithMaxTokens(5), WithOverlapTokens(2))

	text := "the cat sat. the dog ran. the fox jumped."
	chunks := c.Chunk(text)

	require.Greater(t, len(chunks), 1, "expected multiple chunks")

	// Verify sequential numbering.
	for i, ch := range chunks {
		require.Equal(t, i, ch.SeqNum)
		require.NotEmpty(t, ch.Text)
	}
}

func TestChunkOverlap(t *testing.T) {
	tok := loadTestTokenizer(t)
	// Each sentence is ~4 tokens. maxTokens=10 means 2 sentences per
	// chunk. overlapTokens=5 means the last sentence of the previous
	// chunk should appear as overlap in the next.
	c := NewChunker(tok, WithMaxTokens(10), WithOverlapTokens(5))

	text := "the cat sat. the dog ran. the fox jumped."
	chunks := c.Chunk(text)

	require.Len(t, chunks, 2)
	// First chunk: "the cat sat. the dog ran."
	require.Contains(t, chunks[0].Text, "the cat sat.")
	require.Contains(t, chunks[0].Text, "the dog ran.")
	// Second chunk should include "the dog ran." as overlap.
	require.Contains(t, chunks[1].Text, "the dog ran.")
	require.Contains(t, chunks[1].Text, "the fox jumped.")
}

func TestChunkNoSentenceBoundaries(t *testing.T) {
	tok := loadTestTokenizer(t)
	// Text without sentence-ending punctuation: the entire text is one
	// "sentence" and should be returned as a single chunk if it fits.
	c := NewChunker(tok, WithMaxTokens(50), WithOverlapTokens(10))

	chunks := c.Chunk("the cat sat on the mat")
	require.Len(t, chunks, 1)
	require.Equal(t, "the cat sat on the mat", chunks[0].Text)
}

func TestChunkForwardProgress(t *testing.T) {
	tok := loadTestTokenizer(t)
	// A single sentence that exceeds maxTokens should still be
	// returned as one chunk (it will be truncated at embedding time).
	c := NewChunker(tok, WithMaxTokens(3), WithOverlapTokens(1))

	chunks := c.Chunk("the cat sat on the mat.")
	require.GreaterOrEqual(t, len(chunks), 1)
	// Ensure no infinite loop — we get a finite number of chunks.
	require.LessOrEqual(t, len(chunks), 10)
}

func TestSplitSentences(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "simple",
			input:    "Hello world. How are you?",
			expected: []string{"Hello world.", "How are you?"},
		},
		{
			name:     "exclamation",
			input:    "Hello! World!",
			expected: []string{"Hello!", "World!"},
		},
		{
			name:     "question",
			input:    "Who? What? Where?",
			expected: []string{"Who?", "What?", "Where?"},
		},
		{
			name:     "no sentence end",
			input:    "hello world",
			expected: []string{"hello world"},
		},
		{
			name:     "trailing text",
			input:    "First sentence. then some text",
			expected: []string{"First sentence.", "then some text"},
		},
		{
			name:     "empty",
			input:    "",
			expected: nil,
		},
		{
			name:     "whitespace only",
			input:    "   ",
			expected: nil,
		},
		{
			name:  "abbreviation-like dots",
			input: "Dr. Smith went home. He was tired.",
			// "Dr." is followed by a space, so it triggers a split.
			// This is a known limitation of simple sentence splitting.
			expected: []string{"Dr.", "Smith went home.", "He was tired."},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitSentences(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestChunkDefaultOptions(t *testing.T) {
	tok := loadTestTokenizer(t)
	c := NewChunker(tok) // defaults: maxTokens=200, overlapTokens=50
	require.Equal(t, 200, c.maxTokens)
	require.Equal(t, 50, c.overlapTokens)
}
