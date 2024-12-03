// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package source

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/model"
)

type file struct {
	Directory string
}

func (f file) Close() {
	// do Nothing
}

func (f file) Start(c chan model.FileInfo) (err error) {

	err = filepath.Walk(f.Directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			if !strings.HasSuffix(info.Name(), ".json") {
				return nil
			}
			content, err := os.ReadFile(path)
			if err != nil {
				fmt.Printf("Failed to read file %s: %v", path, err)
				return nil // Continue walking the directory tree
			}
			if !strings.HasSuffix(f.Directory, `/`) {
				f.Directory = f.Directory + `/`
			}
			c <- model.FileInfo{
				Path:    strings.TrimPrefix(path, f.Directory),
				Content: content,
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("error walking the path %q: %w", f.Directory, err)
	}

	return err
}

func NewFile(directory string) Source {
	return &file{
		Directory: directory,
	}
}
