// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package chunker splits text into overlapping segments suitable for
// embedding with transformer models that have a fixed token limit. It
// uses sentence-aware splitting: text is first split on sentence
// boundaries, then sentences are greedily packed into chunks up to a
// configurable token limit, with overlap for context continuity.
package chunker

import (
	"strings"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/embedding/tokenizer"
)

// Chunk represents a segment of text extracted from a larger document.
type Chunk struct {
	// Text is the chunk content.
	Text string
	// SeqNum is the 0-based position of this chunk within the original
	// document.
	SeqNum int
}

// Chunker splits text into overlapping chunks suitable for embedding.
// It is safe for concurrent use after construction.
type Chunker struct {
	tok           *tokenizer.Tokenizer
	maxTokens     int
	overlapTokens int
}

// Option configures the Chunker.
type Option func(*Chunker)

// WithMaxTokens sets the maximum number of tokens per chunk. This
// should leave room for [CLS] and [SEP] tokens (subtracted
// internally). The default is 200.
func WithMaxTokens(n int) Option {
	return func(c *Chunker) { c.maxTokens = n }
}

// WithOverlapTokens sets the number of overlapping tokens between
// consecutive chunks. Overlap provides context continuity across chunk
// boundaries. The default is 50.
func WithOverlapTokens(n int) Option {
	return func(c *Chunker) { c.overlapTokens = n }
}

// NewChunker creates a Chunker that uses the given tokenizer for token
// counting. The defaults are: maxTokens=200, overlapTokens=50.
func NewChunker(tok *tokenizer.Tokenizer, opts ...Option) *Chunker {
	c := &Chunker{
		tok:           tok,
		maxTokens:     200,
		overlapTokens: 50,
	}
	for _, apply := range opts {
		apply(c)
	}
	return c
}

// Chunk splits text into overlapping chunks. If the text fits within a
// single chunk, a single-element slice is returned. Empty text returns
// a single chunk with empty text (SeqNum=0).
func (c *Chunker) Chunk(text string) []Chunk {
	sentences := splitSentences(text)
	if len(sentences) == 0 {
		return []Chunk{{Text: "", SeqNum: 0}}
	}

	// Count tokens for each sentence.
	tokenCounts := make([]int, len(sentences))
	for i, s := range sentences {
		tokenCounts[i] = c.tok.TokenCount(s)
	}

	var chunks []Chunk
	seq := 0
	start := 0

	for start < len(sentences) {
		// Greedily pack sentences into a chunk up to maxTokens.
		tokens := 0
		end := start
		for end < len(sentences) {
			added := tokenCounts[end]
			if tokens+added > c.maxTokens && end > start {
				// Adding this sentence would exceed the limit, and we
				// already have at least one sentence.
				break
			}
			tokens += added
			end++
			// If a single sentence exceeds maxTokens, include it alone
			// (it will be truncated during tokenization).
			if tokens >= c.maxTokens {
				break
			}
		}

		chunks = append(chunks, Chunk{
			Text:   joinSentences(sentences[start:end]),
			SeqNum: seq,
		})
		seq++

		if end >= len(sentences) {
			break
		}

		// Find the overlap start: walk backwards from end to find
		// sentences that fit within overlapTokens.
		overlapStart := end
		overlapTokens := 0
		for overlapStart > start {
			candidate := tokenCounts[overlapStart-1]
			if overlapTokens+candidate > c.overlapTokens {
				break
			}
			overlapTokens += candidate
			overlapStart--
		}
		// Ensure forward progress: if overlap would put us back at
		// start, advance at least to end.
		if overlapStart <= start {
			overlapStart = end
		}
		start = overlapStart
	}

	return chunks
}

// splitSentences splits text on sentence boundaries. A sentence
// boundary is defined as a period, exclamation mark, or question mark
// followed by whitespace or end of string. The delimiter is kept with
// the preceding sentence.
func splitSentences(text string) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}

	var sentences []string
	runes := []rune(text)
	start := 0

	for i := 0; i < len(runes); i++ {
		if isSentenceEnd(runes[i]) {
			// Look ahead for whitespace or end of string.
			nextIdx := i + 1
			if nextIdx >= len(runes) || unicode.IsSpace(runes[nextIdx]) {
				sentence := strings.TrimSpace(string(runes[start : i+1]))
				if sentence != "" {
					sentences = append(sentences, sentence)
				}
				start = nextIdx
			}
		}
	}

	// Remaining text after the last sentence boundary.
	if start < len(runes) {
		remaining := strings.TrimSpace(string(runes[start:]))
		if remaining != "" {
			sentences = append(sentences, remaining)
		}
	}

	return sentences
}

// isSentenceEnd returns true if r is a sentence-ending punctuation
// mark.
func isSentenceEnd(r rune) bool {
	return r == '.' || r == '!' || r == '?'
}

// joinSentences joins sentences with a single space separator.
func joinSentences(sentences []string) string {
	return strings.Join(sentences, " ")
}
