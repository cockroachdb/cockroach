// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ssh

import (
	"os"
	"testing"
)

func TestProgress(t *testing.T) {
	output, err := os.CreateTemp("", "example*")
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	defer func() {
		if err := os.Remove(output.Name()); err != nil {
			t.Fatal(err)
		}
	}()

	b := make([]byte, 10)
	var percent float64
	writer := &ProgressWriter{
		Writer: output,
		Done:   0,
		Total:  50,
		Progress: func(currentProgress float64) {
			percent = currentProgress
		},
	}
	for i := 0; i < 4; i++ {
		if _, err := writer.Write(b); err != nil {
			t.Fatal(err)
		}
	}
	if percent != 0.8 {
		t.Errorf("expected progress of 80%% but got %.2f", percent*100)
	}
	if _, err := writer.Write(b); err != nil {
		t.Fatal(err)
	}
	if percent != 1.0 {
		t.Errorf("expected progress of 100%% but got %.2f", percent*100)
	}
}
