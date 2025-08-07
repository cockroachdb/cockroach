// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tsdumpmeta

import (
	"encoding/gob"
	"io"
	"time"
)

// Metadata contains metadata that can be embedded at the start of a raw tsdump
// GOB stream. It includes a store-to-node mapping and auxiliary information.
type Metadata struct {
	Version        string            `json:"version"`
	StoreToNodeMap map[string]string `json:"storeToNodeMap"`
	CreatedAt      time.Time         `json:"createdAt"`
}

// Write encodes the provided metadata and writes it to w using gob.
func Write(w io.Writer, md Metadata) error {
	enc := gob.NewEncoder(w)
	return enc.Encode(md)
}

// Read attempts to read a Metadata from the provided gob.Decoder and returns the
// decoded Metadata. If the first entry is not a Metadata, an error is returned and
// the decoder will have consumed one entry from the stream.
func Read(dec *gob.Decoder) (*Metadata, error) {
	var md Metadata
	if err := dec.Decode(&md); err != nil {
		return nil, err
	}
	return &md, nil
}
