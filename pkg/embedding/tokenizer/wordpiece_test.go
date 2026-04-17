// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tokenizer

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func loadTestVocab(t *testing.T) *Vocab {
	t.Helper()
	f, err := os.Open("testdata/tiny_vocab.txt")
	require.NoError(t, err)
	defer f.Close()
	v, err := LoadVocab(f)
	require.NoError(t, err)
	return v
}

func TestWordPieceTokenize(t *testing.T) {
	v := loadTestVocab(t)

	t.Run("known_word", func(t *testing.T) {
		// "hello" is in the vocab as a whole token.
		ids := wordPieceTokenize("hello", v, 200)
		require.Equal(t, []int64{v.TokenToID("hello")}, ids)
	})

	t.Run("subword_split", func(t *testing.T) {
		// "unable" should split into "un" + "##able".
		ids := wordPieceTokenize("unable", v, 200)
		require.Equal(t, []int64{
			v.TokenToID("un"),
			v.TokenToID("##able"),
		}, ids)
	})

	t.Run("multi_subword", func(t *testing.T) {
		// "uning" should split into "un" + "##ing" (both in vocab).
		ids := wordPieceTokenize("uning", v, 200)
		require.Equal(t, []int64{
			v.TokenToID("un"),
			v.TokenToID("##ing"),
		}, ids)
	})

	t.Run("partial_unknown", func(t *testing.T) {
		// "unabled" → "un" + "##able" + "##d", but "##d" is not
		// in vocab, so the whole word becomes [UNK].
		ids := wordPieceTokenize("unabled", v, 200)
		require.Equal(t, []int64{v.unkID}, ids)
	})

	t.Run("unknown_word", func(t *testing.T) {
		// A word with characters not in the vocab at all.
		ids := wordPieceTokenize("xyz123", v, 200)
		require.Equal(t, []int64{v.unkID}, ids)
	})

	t.Run("single_char_known", func(t *testing.T) {
		// Single character 'a' is in the vocab.
		ids := wordPieceTokenize("a", v, 200)
		require.Equal(t, []int64{v.TokenToID("a")}, ids)
	})

	t.Run("too_long", func(t *testing.T) {
		// Word exceeding maxWordLen should return [UNK].
		long := make([]rune, 201)
		for i := range long {
			long[i] = 'a'
		}
		ids := wordPieceTokenize(string(long), v, 200)
		require.Equal(t, []int64{v.unkID}, ids)
	})

	t.Run("empty_word", func(t *testing.T) {
		ids := wordPieceTokenize("", v, 200)
		require.Empty(t, ids)
	})
}
