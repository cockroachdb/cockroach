// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tokenizer

// wordPieceTokenize splits a single pre-tokenized word into subword
// token IDs using the greedy longest-match-first algorithm. If the word
// cannot be decomposed into known vocabulary entries, a single [UNK]
// token ID is returned.
//
// The algorithm operates on runes (not bytes) to handle Unicode correctly.
// Continuation subwords are prefixed with "##" before vocabulary lookup.
func wordPieceTokenize(word string, vocab *Vocab, maxWordLen int) []int64 {
	runes := []rune(word)
	if len(runes) > maxWordLen {
		return []int64{vocab.unkID}
	}

	var tokens []int64
	start := 0
	for start < len(runes) {
		matched := false
		for end := len(runes); end > start; end-- {
			substr := string(runes[start:end])
			if start > 0 {
				substr = "##" + substr
			}
			if _, ok := vocab.tokenToID[substr]; ok {
				tokens = append(tokens, vocab.tokenToID[substr])
				start = end
				matched = true
				break
			}
		}
		if !matched {
			// No subword found at this position — the entire word
			// is unknown.
			return []int64{vocab.unkID}
		}
	}
	return tokens
}
