// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package extract

import (
	"bytes"
	"io"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/net/html"
)

// XHTMLtoHTML converts the XHTML railroad diagrams to HTML.
func XHTMLtoHTML(r io.Reader) (string, error) {
	b := new(bytes.Buffer)
	z := html.NewTokenizer(r)
	for {
		tt := z.Next()
		if tt == html.ErrorToken {
			err := z.Err()
			if err == io.EOF {
				break
			}
			return "", z.Err()
		}
		t := z.Token()
		switch t.Type {
		case html.StartTagToken, html.EndTagToken, html.SelfClosingTagToken:
			idx := strings.Index(t.Data, ":")
			t.Data = t.Data[idx+1:]
		}
		var na []html.Attribute
		for _, a := range t.Attr {
			if strings.HasPrefix(a.Key, "xmlns") {
				continue
			}
			na = append(na, a)
		}
		t.Attr = na
		b.WriteString(t.String())
	}

	doc, err := goquery.NewDocumentFromReader(b)
	if err != nil {
		return "", err
	}
	defs := doc.Find("defs")
	dhtml, err := defs.First().Html()
	if err != nil {
		return "", err
	}
	doc.Find("head").AppendHtml(dhtml)
	defs.Remove()
	doc.Find("svg").First().Remove()
	doc.Find("meta[http-equiv]").Remove()
	doc.Find("head").PrependHtml(`<meta charset="UTF-8">`)
	doc.Find("a[name]:not([href])").Each(func(_ int, s *goquery.Selection) {
		name, exists := s.Attr("name")
		if !exists {
			return
		}
		s.SetAttr("href", "#"+name)
	})
	s, err := doc.Find("html").Html()
	s = "<!DOCTYPE html><html>" + s + "</html>"
	return s, err
}

// Tag returns the tag contents of r.
func Tag(r io.Reader, tag string) (string, error) {
	doc, err := goquery.NewDocumentFromReader(r)
	if err != nil {
		return "", err
	}
	node := doc.Find(tag).Get(0)
	var b bytes.Buffer
	if err := html.Render(&b, node); err != nil {
		return "", err
	}
	return b.String(), nil
}

// InnerTag returns the inner contents of <tag> from r.
func InnerTag(r io.Reader, tag string) (string, error) {
	doc, err := goquery.NewDocumentFromReader(r)
	if err != nil {
		return "", err
	}
	return doc.Find(tag).Html()
}
