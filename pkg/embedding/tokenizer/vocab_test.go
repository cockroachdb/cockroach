// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tokenizer

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadVocab(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		f, err := os.Open("testdata/tiny_vocab.txt")
		require.NoError(t, err)
		defer f.Close()

		v, err := LoadVocab(f)
		require.NoError(t, err)
		require.Equal(t, 48, v.Size())

		// Special tokens at expected positions.
		require.Equal(t, int64(0), v.padID)
		require.Equal(t, int64(1), v.unkID)
		require.Equal(t, int64(2), v.clsID)
		require.Equal(t, int64(3), v.sepID)

		// Known tokens (0-indexed line numbers).
		require.Equal(t, int64(31), v.TokenToID("hello"))
		require.Equal(t, int64(32), v.TokenToID("world"))
		require.Equal(t, int64(36), v.TokenToID("##able"))

		// Unknown token falls back to [UNK].
		require.Equal(t, v.unkID, v.TokenToID("nonexistent"))
	})

	t.Run("missing_special_tokens", func(t *testing.T) {
		// Vocabulary without [CLS].
		input := "[PAD]\n[UNK]\n[SEP]\nhello\n"
		_, err := LoadVocab(strings.NewReader(input))
		require.Error(t, err)
		require.Contains(t, err.Error(), "[CLS]")
	})

	t.Run("missing_unk", func(t *testing.T) {
		input := "[PAD]\n[CLS]\n[SEP]\nhello\n"
		_, err := LoadVocab(strings.NewReader(input))
		require.Error(t, err)
		require.Contains(t, err.Error(), "[UNK]")
	})

	t.Run("empty_reader", func(t *testing.T) {
		_, err := LoadVocab(strings.NewReader(""))
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty")
	})
}
