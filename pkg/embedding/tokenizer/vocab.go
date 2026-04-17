// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tokenizer

import (
	"bufio"
	"io"

	"github.com/cockroachdb/errors"
)

// Vocab holds the mapping between tokens and their integer IDs. It is
// loaded from a vocab.txt file where each line contains a single token
// and the 0-indexed line number is the token ID.
//
// A Vocab is immutable after construction and safe for concurrent use.
type Vocab struct {
	tokenToID map[string]int64
	idToToken []string
	unkID     int64
	clsID     int64
	sepID     int64
	padID     int64
}

// LoadVocab reads a vocabulary file from r. The format is one token per
// line, where the 0-indexed line number is the token's integer ID. The
// vocabulary must contain the special tokens [PAD], [UNK], [CLS], and
// [SEP].
func LoadVocab(r io.Reader) (*Vocab, error) {
	v := &Vocab{
		tokenToID: make(map[string]int64),
		unkID:     -1,
		clsID:     -1,
		sepID:     -1,
		padID:     -1,
	}
	scanner := bufio.NewScanner(r)
	var id int64
	for scanner.Scan() {
		token := scanner.Text()
		v.tokenToID[token] = id
		v.idToToken = append(v.idToToken, token)
		switch token {
		case "[PAD]":
			v.padID = id
		case "[UNK]":
			v.unkID = id
		case "[CLS]":
			v.clsID = id
		case "[SEP]":
			v.sepID = id
		}
		id++
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.Wrap(err, "reading vocabulary")
	}
	if len(v.idToToken) == 0 {
		return nil, errors.New("vocabulary is empty")
	}
	if v.padID < 0 {
		return nil, errors.New("vocabulary missing [PAD] token")
	}
	if v.unkID < 0 {
		return nil, errors.New("vocabulary missing [UNK] token")
	}
	if v.clsID < 0 {
		return nil, errors.New("vocabulary missing [CLS] token")
	}
	if v.sepID < 0 {
		return nil, errors.New("vocabulary missing [SEP] token")
	}
	return v, nil
}

// TokenToID returns the integer ID for the given token. If the token is
// not in the vocabulary, the [UNK] token ID is returned.
func (v *Vocab) TokenToID(token string) int64 {
	if id, ok := v.tokenToID[token]; ok {
		return id
	}
	return v.unkID
}

// Size returns the number of tokens in the vocabulary.
func (v *Vocab) Size() int {
	return len(v.idToToken)
}
