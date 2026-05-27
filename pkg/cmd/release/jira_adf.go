// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

// Helpers for building Atlassian Document Format (ADF) values to send to the
// Jira REST v3 comment API. ADF is a JSON tree of typed nodes; we model it as
// nested map[string]interface{} since the surface we use is small enough that
// a typed schema would be more boilerplate than help.
//
// Spec: https://developer.atlassian.com/cloud/jira/platform/apis/document/structure/

// adfDoc wraps a sequence of block-level nodes in the top-level doc envelope
// that the comment API expects.
func adfDoc(blocks ...interface{}) map[string]interface{} {
	return map[string]interface{}{
		"type":    "doc",
		"version": 1,
		"content": blocks,
	}
}

// adfPara builds a paragraph block from the given inline nodes.
func adfPara(inlines ...interface{}) map[string]interface{} {
	return map[string]interface{}{
		"type":    "paragraph",
		"content": inlines,
	}
}

// adfBullet builds a bullet list. Each argument is the slice of inline nodes
// for one item; the helper wraps each in the listItem/paragraph nesting that
// ADF requires.
func adfBullet(items ...[]interface{}) map[string]interface{} {
	listItems := make([]interface{}, len(items))
	for i, item := range items {
		listItems[i] = map[string]interface{}{
			"type":    "listItem",
			"content": []interface{}{adfPara(item...)},
		}
	}
	return map[string]interface{}{
		"type":    "bulletList",
		"content": listItems,
	}
}

// adfText returns a plain text inline node.
func adfText(s string) map[string]interface{} {
	return map[string]interface{}{"type": "text", "text": s}
}

// adfMarked returns a text inline node with one or more marks
// (strong, code, link, ...) applied.
func adfMarked(s string, marks ...map[string]interface{}) map[string]interface{} {
	m := make([]interface{}, len(marks))
	for i, mark := range marks {
		m[i] = mark
	}
	return map[string]interface{}{
		"type":  "text",
		"text":  s,
		"marks": m,
	}
}

func adfStrong() map[string]interface{} { return map[string]interface{}{"type": "strong"} }
func adfCode() map[string]interface{}   { return map[string]interface{}{"type": "code"} }
func adfLink(href string) map[string]interface{} {
	return map[string]interface{}{
		"type":  "link",
		"attrs": map[string]interface{}{"href": href},
	}
}
