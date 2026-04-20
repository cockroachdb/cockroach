// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package tokenizer implements a WordPiece tokenizer compatible with
// BERT uncased models. It converts raw text into integer token IDs and
// attention masks suitable for input to transformer ONNX models.
package tokenizer

// EncodingResult holds the output of tokenizing a single text input.
type EncodingResult struct {
	// InputIDs contains the token IDs, including [CLS], [SEP], and
	// [PAD] tokens, with length equal to the tokenizer's maxSeqLen.
	InputIDs []int64
	// AttentionMask has the same length as InputIDs. Each element is 1
	// for real tokens and 0 for padding tokens.
	AttentionMask []int64
	// TokenCount is the number of real tokens (including [CLS] and
	// [SEP]) before padding was applied.
	TokenCount int
}

type options struct {
	maxSeqLen  int
	doLower    bool
	maxWordLen int
}

// Option configures the Tokenizer.
type Option func(*options)

// WithMaxSeqLen sets the maximum sequence length. Inputs longer than
// this (after tokenization) are truncated. Shorter inputs are padded.
// The default is 256.
func WithMaxSeqLen(n int) Option {
	return func(o *options) { o.maxSeqLen = n }
}

// WithLowerCase controls whether input text is lowercased and accents
// are stripped before tokenization. The default is true (BERT uncased).
func WithLowerCase(b bool) Option {
	return func(o *options) { o.doLower = b }
}

// WithMaxWordLen sets the maximum word length (in runes) for WordPiece
// tokenization. Words longer than this are mapped to [UNK]. The default
// is 200.
func WithMaxWordLen(n int) Option {
	return func(o *options) { o.maxWordLen = n }
}

// Tokenizer converts raw text into token IDs for transformer models.
// It is safe for concurrent use after construction.
type Tokenizer struct {
	vocab *Vocab
	opts  options
}

// NewTokenizer creates a Tokenizer with the given vocabulary and
// options. The defaults are: maxSeqLen=256, doLower=true,
// maxWordLen=200.
func NewTokenizer(vocab *Vocab, opts ...Option) *Tokenizer {
	o := options{
		maxSeqLen:  256,
		doLower:    true,
		maxWordLen: 200,
	}
	for _, apply := range opts {
		apply(&o)
	}
	return &Tokenizer{vocab: vocab, opts: o}
}

// Encode tokenizes a single text string and returns the token IDs,
// attention mask, and real token count. The output is padded to
// maxSeqLen.
func (t *Tokenizer) Encode(text string) EncodingResult {
	ids := t.tokenizeToIDs(text)
	seqLen := t.opts.maxSeqLen
	return t.padAndWrap(ids, seqLen)
}

// EncodeBatch tokenizes multiple texts and returns flat slices suitable
// for batched model inference. The sequences are padded to the length
// of the longest sequence in the batch (capped at maxSeqLen), which is
// more efficient than always padding to maxSeqLen.
//
// The returned inputIDs and attentionMask slices have length
// batchSize * seqLen.
func (t *Tokenizer) EncodeBatch(
	texts []string,
) (inputIDs, attentionMask []int64, batchSize, seqLen int) {
	batchSize = len(texts)
	if batchSize == 0 {
		return nil, nil, 0, 0
	}

	// Tokenize all texts without padding.
	allIDs := make([][]int64, batchSize)
	maxLen := 0
	for i, text := range texts {
		allIDs[i] = t.tokenizeToIDs(text)
		// +2 for [CLS] and [SEP].
		tokenLen := len(allIDs[i]) + 2
		if tokenLen > maxLen {
			maxLen = tokenLen
		}
	}

	// Cap at maxSeqLen.
	seqLen = maxLen
	if seqLen > t.opts.maxSeqLen {
		seqLen = t.opts.maxSeqLen
	}

	// Pad each sequence and flatten.
	inputIDs = make([]int64, 0, batchSize*seqLen)
	attentionMask = make([]int64, 0, batchSize*seqLen)
	for _, ids := range allIDs {
		enc := t.padAndWrap(ids, seqLen)
		inputIDs = append(inputIDs, enc.InputIDs...)
		attentionMask = append(attentionMask, enc.AttentionMask...)
	}
	return inputIDs, attentionMask, batchSize, seqLen
}

// TokenCount returns the number of subword tokens the tokenizer would
// produce for the given text, excluding [CLS], [SEP], and padding.
// This is useful for chunking decisions without full encoding overhead.
func (t *Tokenizer) TokenCount(text string) int {
	return len(t.tokenizeToIDs(text))
}

// tokenizeToIDs runs pre-tokenization and WordPiece on the input text,
// returning the raw subword IDs without [CLS], [SEP], or padding.
func (t *Tokenizer) tokenizeToIDs(text string) []int64 {
	words := bertPreTokenize(text, t.opts.doLower)
	var ids []int64
	for _, word := range words {
		ids = append(ids, wordPieceTokenize(word, t.vocab, t.opts.maxWordLen)...)
	}
	return ids
}

// padAndWrap adds [CLS] and [SEP] tokens, truncates if necessary, and
// pads to seqLen.
func (t *Tokenizer) padAndWrap(ids []int64, seqLen int) EncodingResult {
	// Truncate to leave room for [CLS] and [SEP].
	maxTokens := seqLen - 2
	if maxTokens < 0 {
		maxTokens = 0
	}
	if len(ids) > maxTokens {
		ids = ids[:maxTokens]
	}

	// Build the full sequence: [CLS] + tokens + [SEP] + padding.
	tokenCount := len(ids) + 2 // +2 for [CLS] and [SEP]
	inputIDs := make([]int64, seqLen)
	attentionMask := make([]int64, seqLen)

	inputIDs[0] = t.vocab.clsID
	attentionMask[0] = 1
	for i, id := range ids {
		inputIDs[i+1] = id
		attentionMask[i+1] = 1
	}
	inputIDs[len(ids)+1] = t.vocab.sepID
	attentionMask[len(ids)+1] = 1

	// Remaining positions are already zero (padID=0 and mask=0).
	// Fill padID explicitly in case padID != 0.
	for i := tokenCount; i < seqLen; i++ {
		inputIDs[i] = t.vocab.padID
	}

	return EncodingResult{
		InputIDs:      inputIDs,
		AttentionMask: attentionMask,
		TokenCount:    tokenCount,
	}
}
