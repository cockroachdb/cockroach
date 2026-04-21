// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package content

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractText(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		uri     string
		want    string
		wantErr string
	}{
		{
			name: "plain text file",
			data: []byte("hello world"),
			uri:  "s3://bucket/docs/hello.txt?AUTH=implicit",
			want: "hello world",
		},
		{
			name: "markdown file",
			data: []byte("# Title\nSome content"),
			uri:  "s3://bucket/readme.md",
			want: "# Title\nSome content",
		},
		{
			name: "csv file",
			data: []byte("a,b,c\n1,2,3"),
			uri:  "nodelocal://1/data.csv",
			want: "a,b,c\n1,2,3",
		},
		{
			name: "no extension treated as text",
			data: []byte("plain content"),
			uri:  "s3://bucket/somefile",
			want: "plain content",
		},
		{
			name:    "invalid pdf returns error",
			data:    []byte("not a real pdf"),
			uri:     "s3://bucket/doc.pdf",
			wantErr: "opening PDF",
		},
		{
			name:    "image not supported",
			data:    []byte{0xFF, 0xD8},
			uri:     "s3://bucket/photo.jpg",
			wantErr: "image embedding is not yet supported",
		},
		{
			name:    "video not supported",
			data:    []byte{0x00},
			uri:     "s3://bucket/video.mp4",
			wantErr: "video/audio embedding is not yet supported",
		},
		{
			name: "json file",
			data: []byte(`{"key": "value"}`),
			uri:  "https://example.com/data.json?token=abc",
			want: `{"key": "value"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExtractText(tt.data, tt.uri)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestExtensionFromURI(t *testing.T) {
	tests := []struct {
		uri  string
		want string
	}{
		{"s3://bucket/file.txt", ".txt"},
		{"s3://bucket/file.txt?AUTH=implicit", ".txt"},
		{"s3://bucket/path/to/doc.PDF", ".pdf"},
		{"s3://bucket/noext", ""},
		{"nodelocal://1/data.csv", ".csv"},
		{"https://example.com/file.md?v=2#section", ".md"},
	}

	for _, tt := range tests {
		t.Run(tt.uri, func(t *testing.T) {
			got := extensionFromURI(tt.uri)
			if !strings.EqualFold(got, tt.want) {
				t.Errorf("extensionFromURI(%q) = %q, want %q", tt.uri, got, tt.want)
			}
		})
	}
}
