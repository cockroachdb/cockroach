// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package content provides content type detection and text extraction
// for the vectorizer's URI loading mode. Given raw file bytes and a
// filename, it detects the content type and extracts text suitable
// for embedding.
package content

import (
	"bytes"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/ledongthuc/pdf"
)

// MaxFileSize is the maximum file size read_uri() will process.
// This is a POC safety limit — production would use streaming.
const MaxFileSize = 64 << 20 // 64MB

// ContentType classifies file content for routing to the appropriate
// embedding pipeline.
type ContentType int

const (
	// ContentTypeText indicates text-extractable content (txt, md, csv, pdf, etc.)
	ContentTypeText ContentType = iota
	// ContentTypeImage indicates image content that needs a multimodal embedder.
	ContentTypeImage
	// ContentTypeUnsupported indicates an unsupported content type.
	ContentTypeUnsupported
)

// ClassifyURI determines the content type from the URI's file extension.
func ClassifyURI(uri string) ContentType {
	ext := extensionFromURI(uri)
	switch ext {
	case ".txt", ".md", ".csv", ".log", ".json", ".xml", ".html", ".yml", ".yaml", ".pdf", "":
		return ContentTypeText
	case ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp":
		return ContentTypeImage
	default:
		return ContentTypeUnsupported
	}
}

// IsImage returns true if the URI points to an image file.
func IsImage(uri string) bool {
	return ClassifyURI(uri) == ContentTypeImage
}

// ExtractText takes raw file content and a URI, detects the content
// type from the URI's file extension, and extracts text. Returns an
// error for unsupported file types.
func ExtractText(data []byte, uri string) (string, error) {
	ext := extensionFromURI(uri)
	switch ext {
	case ".txt", ".md", ".csv", ".log", ".json", ".xml", ".html", ".yml", ".yaml", "":
		return string(data), nil
	case ".pdf":
		return extractPDFText(data)
	case ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp":
		return "", errors.WithHint(
			errors.Newf("image embedding is not yet supported (file type: %s)", ext),
			"Image support requires a multimodal embedding model.",
		)
	case ".mp4", ".avi", ".mov", ".mp3", ".wav":
		return "", errors.WithHint(
			errors.Newf("video/audio embedding is not yet supported (file type: %s)", ext),
			"Video/audio support requires a transcription model.",
		)
	default:
		return "", errors.Newf("unsupported file type: %s", ext)
	}
}

// extractPDFText extracts plain text from PDF file bytes using the
// ledongthuc/pdf library. It reads all pages and concatenates text
// with newlines between pages.
func extractPDFText(data []byte) (string, error) {
	r, err := pdf.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return "", errors.Wrap(err, "opening PDF")
	}
	var sb strings.Builder
	for i := 1; i <= r.NumPage(); i++ {
		page := r.Page(i)
		if page.V.IsNull() {
			continue
		}
		text, err := page.GetPlainText(nil)
		if err != nil {
			return "", errors.Wrapf(err, "extracting text from PDF page %d", i)
		}
		if sb.Len() > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString(text)
	}
	if sb.Len() == 0 {
		return "", errors.New("PDF contains no extractable text")
	}
	return sb.String(), nil
}

// extensionFromURI extracts the file extension from a URI, stripping
// any query parameters. For example, "s3://bucket/doc.txt?AUTH=implicit"
// returns ".txt".
func extensionFromURI(uri string) string {
	// Strip query parameters and fragments.
	parsed, err := url.Parse(uri)
	if err != nil {
		// Fall back to simple extension extraction.
		return strings.ToLower(filepath.Ext(uri))
	}
	return strings.ToLower(filepath.Ext(parsed.Path))
}
