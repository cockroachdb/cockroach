// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tsearch

import (
	"bytes"
	"embed"
	"path"
	"strings"
)

//go:embed stopwords/*
var stopwordFS embed.FS

var stopwordsMap map[string]map[string]struct{}

func init() {
	stopwordsMap = make(map[string]map[string]struct{})
	dir, err := stopwordFS.ReadDir("stopwords")
	if err != nil {
		panic("error loading stopwords: " + err.Error())
	}
	for _, f := range dir {
		filename := f.Name()
		name := strings.TrimSuffix(filename, ".stop")
		// N.B. we use path.Join here instead of filepath.Join because go:embed
		// always uses forward slashes. https://github.com/golang/go/issues/45230
		contents, err := stopwordFS.ReadFile(path.Join("stopwords", filename))
		if err != nil {
			panic("error loading stopwords: " + err.Error())
		}
		wordList := bytes.Fields(contents)
		stopwordsMap[name] = make(map[string]struct{}, len(wordList))
		for _, word := range wordList {
			stopwordsMap[name][string(word)] = struct{}{}
		}
	}
	// The simple text search config has no stopwords.
	stopwordsMap["simple"] = nil
}
