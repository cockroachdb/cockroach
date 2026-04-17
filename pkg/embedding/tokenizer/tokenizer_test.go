// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tokenizer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncode(t *testing.T) {
	v := loadTestVocab(t)
	tok := NewTokenizer(v, WithMaxSeqLen(10))

	t.Run("basic", func(t *testing.T) {
		enc := tok.Encode("hello world")
		// [CLS] hello world [SEP] [PAD] [PAD] [PAD] [PAD] [PAD] [PAD]
		require.Len(t, enc.InputIDs, 10)
		require.Len(t, enc.AttentionMask, 10)
		require.Equal(t, v.clsID, enc.InputIDs[0])
		require.Equal(t, v.TokenToID("hello"), enc.InputIDs[1])
		require.Equal(t, v.TokenToID("world"), enc.InputIDs[2])
		require.Equal(t, v.sepID, enc.InputIDs[3])
		require.Equal(t, 4, enc.TokenCount)

		// Attention mask: 1 for real tokens, 0 for padding.
		for i := 0; i < 4; i++ {
			require.Equal(t, int64(1), enc.AttentionMask[i])
		}
		for i := 4; i < 10; i++ {
			require.Equal(t, int64(0), enc.AttentionMask[i])
			require.Equal(t, v.padID, enc.InputIDs[i])
		}
	})

	t.Run("truncation", func(t *testing.T) {
		// With maxSeqLen=5 and 3 token slots (minus [CLS]+[SEP]),
		// a long input should be truncated.
		shortTok := NewTokenizer(v, WithMaxSeqLen(5))
		enc := shortTok.Encode("hello world the is un")
		// maxTokens = 5-2 = 3, so only first 3 subword tokens kept.
		require.Len(t, enc.InputIDs, 5)
		require.Equal(t, v.clsID, enc.InputIDs[0])
		require.Equal(t, v.sepID, enc.InputIDs[4])
		require.Equal(t, 5, enc.TokenCount)
	})

	t.Run("empty_string", func(t *testing.T) {
		enc := tok.Encode("")
		// Should be [CLS] [SEP] + padding.
		require.Len(t, enc.InputIDs, 10)
		require.Equal(t, v.clsID, enc.InputIDs[0])
		require.Equal(t, v.sepID, enc.InputIDs[1])
		require.Equal(t, 2, enc.TokenCount)
		for i := 2; i < 10; i++ {
			require.Equal(t, int64(0), enc.AttentionMask[i])
		}
	})

	t.Run("with_punctuation", func(t *testing.T) {
		enc := tok.Encode("hello, world!")
		// "hello" "," "world" "!" → 4 content tokens + [CLS] + [SEP] = 6.
		require.Equal(t, v.clsID, enc.InputIDs[0])
		require.Equal(t, v.TokenToID("hello"), enc.InputIDs[1])
		require.Equal(t, v.TokenToID(","), enc.InputIDs[2])
		require.Equal(t, v.TokenToID("world"), enc.InputIDs[3])
		require.Equal(t, v.TokenToID("!"), enc.InputIDs[4])
		require.Equal(t, v.sepID, enc.InputIDs[5])
		require.Equal(t, 6, enc.TokenCount)
	})

	t.Run("unknown_words", func(t *testing.T) {
		enc := tok.Encode("zzzzz")
		// "zzzzz" is not in vocab and can't be split → [UNK].
		require.Equal(t, v.clsID, enc.InputIDs[0])
		require.Equal(t, v.unkID, enc.InputIDs[1])
		require.Equal(t, v.sepID, enc.InputIDs[2])
		require.Equal(t, 3, enc.TokenCount)
	})
}

func TestEncodeBatch(t *testing.T) {
	v := loadTestVocab(t)
	tok := NewTokenizer(v, WithMaxSeqLen(10))

	t.Run("dynamic_padding", func(t *testing.T) {
		inputIDs, attentionMask, batchSize, seqLen :=
			tok.EncodeBatch([]string{"hello", "hello world"})

		require.Equal(t, 2, batchSize)
		// "hello" → [CLS] hello [SEP] = 3 tokens
		// "hello world" → [CLS] hello world [SEP] = 4 tokens
		// Dynamic padding → seqLen = 4.
		require.Equal(t, 4, seqLen)
		require.Len(t, inputIDs, 8)      // 2 * 4
		require.Len(t, attentionMask, 8) // 2 * 4

		// First sequence: [CLS] hello [SEP] [PAD]
		require.Equal(t, v.clsID, inputIDs[0])
		require.Equal(t, v.TokenToID("hello"), inputIDs[1])
		require.Equal(t, v.sepID, inputIDs[2])
		require.Equal(t, v.padID, inputIDs[3])
		require.Equal(t, int64(0), attentionMask[3])

		// Second sequence: [CLS] hello world [SEP]
		require.Equal(t, v.clsID, inputIDs[4])
		require.Equal(t, v.TokenToID("hello"), inputIDs[5])
		require.Equal(t, v.TokenToID("world"), inputIDs[6])
		require.Equal(t, v.sepID, inputIDs[7])
	})

	t.Run("single_item", func(t *testing.T) {
		inputIDs, attentionMask, batchSize, seqLen :=
			tok.EncodeBatch([]string{"hello"})
		require.Equal(t, 1, batchSize)
		require.Equal(t, 3, seqLen) // [CLS] hello [SEP]
		require.Len(t, inputIDs, 3)
		require.Len(t, attentionMask, 3)
		// All tokens are real.
		for i := 0; i < 3; i++ {
			require.Equal(t, int64(1), attentionMask[i])
		}
	})

	t.Run("empty_batch", func(t *testing.T) {
		inputIDs, attentionMask, batchSize, seqLen :=
			tok.EncodeBatch(nil)
		require.Equal(t, 0, batchSize)
		require.Equal(t, 0, seqLen)
		require.Nil(t, inputIDs)
		require.Nil(t, attentionMask)
	})
}
