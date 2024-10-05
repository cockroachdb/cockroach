// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metamorphic

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

// parseOutputPreamble reads the commented preamble of an output.meta file,
// paring out the engine configuration.
func parseOutputPreamble(f io.Reader) (cfg engineConfig, seed int64, err error) {
	r := bufio.NewReader(f)

	seed, err = readCommentInt64(r, "seed:")
	if err != nil {
		return cfg, seed, err
	}
	cfg.name, err = readCommentString(r, "name:")
	if err != nil {
		return cfg, seed, err
	}
	if _, err = readCommentString(r, "engine options:"); err != nil {
		return cfg, seed, err
	}

	var optsBuf bytes.Buffer
	for {
		// Read the first byte to check if this line is a comment.
		if firstByte, err := r.ReadByte(); err != nil {
			if err == io.EOF {
				break
			}
			return cfg, seed, err
		} else if firstByte != '#' {
			// The end of the comment preamble.
			break
		}

		b, err := r.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return cfg, seed, err
		}
		optsBuf.Write(b)
	}
	cfg.opts = storage.DefaultPebbleOptions()
	err = cfg.opts.Parse(optsBuf.String(), parseHooks)
	return cfg, seed, err
}

var parseHooks = &pebble.ParseHooks{
	NewFilterPolicy: func(name string) (pebble.FilterPolicy, error) {
		switch name {
		case "none":
			return nil, nil
		case "rocksdb.BuiltinBloomFilter":
			return bloom.FilterPolicy(10), nil
		}
		return nil, nil
	},
}

func readCommentString(r *bufio.Reader, prefix string) (string, error) {
	firstByte, err := r.ReadByte()
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		return "", err
	}
	if firstByte != '#' {
		return "", fmt.Errorf("expected comment with prefix %q, but not a comment", prefix)
	}
	s, err := r.ReadString('\n')
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, prefix)
	s = strings.TrimSpace(s)
	return s, err
}

func readCommentInt64(r *bufio.Reader, prefix string) (int64, error) {
	s, err := readCommentString(r, prefix)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(s, 10, 64)
}
