// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sink

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type file struct {
	directory string
}

func NewFileSink(directory string) *file {
	return &file{
		directory: directory,
	}
}
func (s *file) Sink(buf *bytes.Buffer, filePath string, fileName string) error {
	if buf == nil {
		return nil
	}
	finalPath := fmt.Sprintf("%s/%s/%s",
		strings.Trim(s.directory, `/`),
		strings.Trim(filePath, `/`),
		strings.Trim(fileName, `/`),
	)
	dir := filepath.Dir(finalPath)

	// Create all necessary directories
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directories: %w", err)
	}

	file, err := os.Create(finalPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	_, err = file.Write(buf.Bytes())
	return err
}

func (s *file) Close() {
	// do Nothing
}
