// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package starlarkutil

import (
	"fmt"
	"os"

	"github.com/google/skylark/syntax"
)

// DownloadableArtifact represents a single URL/SHA256 pair.
type DownloadableArtifact struct {
	URL    string
	Sha256 string
}

// ListArtifactsInDepsBzl parses the DEPS.bzl file and returns a map of repo name ->
// DownloadableArtifact capturing what's in that repo.
func ListArtifactsInDepsBzl(depsBzl string) (map[string]DownloadableArtifact, error) {
	in, err := os.Open(depsBzl)
	if err != nil {
		return nil, err
	}
	defer in.Close()
	return downloadableArtifactsFromDepsBzl(in)
}

// GetFunctionFromCall returns the name of the function called for the given
// call expression, or the empty string if the function being called is not a
// normal identifier.
func GetFunctionFromCall(call *syntax.CallExpr) string {
	fn, err := ExpectIdent(call.Fn)
	if err != nil {
		return ""
	}
	return fn
}

// GetArtifactFromHTTPArchive returns the DownloadableArtifact for a given
// http_archive() call.
func GetArtifactFromHTTPArchive(call *syntax.CallExpr) (DownloadableArtifact, error) {
	if GetFunctionFromCall(call) != "http_archive" {
		return DownloadableArtifact{}, fmt.Errorf("expected given call to be an http_archive")
	}
	_, artifact, err := maybeGetDownloadableArtifact(call)
	if err != nil {
		return artifact, err
	}
	if artifact.URL == "" || artifact.Sha256 == "" {
		err = fmt.Errorf("could not determine the url or sha256 for the given http_archive")
	}
	return artifact, err
}

func downloadableArtifactsFromDepsBzl(in interface{}) (map[string]DownloadableArtifact, error) {
	parsed, err := syntax.Parse("DEPS.bzl", in, 0)
	if err != nil {
		return nil, err
	}
	for _, stmt := range parsed.Stmts {
		switch s := stmt.(type) {
		case *syntax.DefStmt:
			if s.Name.Name == "go_deps" {
				return downloadableArtifactsFromGoDeps(s)
			}
		default:
			continue
		}
	}
	return nil, fmt.Errorf("could not find go_deps function in DEPS.bzl")
}

func downloadableArtifactsFromGoDeps(def *syntax.DefStmt) (map[string]DownloadableArtifact, error) {
	ret := make(map[string]DownloadableArtifact)
	for _, stmt := range def.Function.Body {
		switch s := stmt.(type) {
		case *syntax.ExprStmt:
			switch x := s.X.(type) {
			case *syntax.CallExpr:
				fn := GetFunctionFromCall(x)
				if fn != "go_repository" {
					return nil, fmt.Errorf("expected go_repository, got %s", fn)
				}
				name, mirror, err := maybeGetDownloadableArtifact(x)
				if err != nil {
					return nil, err
				}
				if name != "" {
					ret[name] = mirror
				}
			default:
				return nil, fmt.Errorf("unexpected expression in DEPS.bzl: %v", x)
			}
		}
	}
	return ret, nil
}

// GetArtifactFromGoRepository returns the DownloadableArtifact pointed to by the given
// go_repository.
func GetArtifactFromGoRepository(call *syntax.CallExpr) (DownloadableArtifact, error) {
	name, art, err := maybeGetDownloadableArtifact(call)
	if err != nil {
		return art, err
	}
	if name == "" {
		return DownloadableArtifact{}, fmt.Errorf("could not parse downloadable artifact from given go_repository call")
	}
	return art, err
}

// maybeGetDownloadableArtifact returns the DownloadableArtifact pointed to by the given
// go_repository or http_archive expression, returning the name of the repo
// and the location of the mirror if one can be found, or the empty string/an
// empty existingMirror if not. Returns an error iff an unrecoverable problem
// occurred.
func maybeGetDownloadableArtifact(call *syntax.CallExpr) (string, DownloadableArtifact, error) {
	var name, sha256, url string
	for _, arg := range call.Args {
		switch bx := arg.(type) {
		case *syntax.BinaryExpr:
			if bx.Op != syntax.EQ {
				return "", DownloadableArtifact{}, fmt.Errorf("unexpected binary expression Op %d", bx.Op)
			}
			kwarg, err := ExpectIdent(bx.X)
			if err != nil {
				return "", DownloadableArtifact{}, err
			}
			if kwarg == "name" {
				name, err = ExpectLiteralString(bx.Y)
				if err != nil {
					return "", DownloadableArtifact{}, err
				}
			}
			if kwarg == "sha256" {
				sha256, err = ExpectLiteralString(bx.Y)
				if err != nil {
					return "", DownloadableArtifact{}, err
				}
			}
			if kwarg == "urls" {
				url, err = ExpectSingletonStringList(bx.Y)
				if err != nil {
					return "", DownloadableArtifact{}, err
				}
			}
			if kwarg == "url" {
				url, err = ExpectLiteralString(bx.Y)
				if err != nil {
					return "", DownloadableArtifact{}, err
				}
			}
		default:
			return "", DownloadableArtifact{}, fmt.Errorf("unexpected expression in DEPS.bzl: %v", bx)
		}
	}
	if url != "" {
		return name, DownloadableArtifact{URL: url, Sha256: sha256}, nil
	}
	return "", DownloadableArtifact{}, nil
}

// ExpectIdent returns an identifier string or an error if this Expr is not an
// identifier.
func ExpectIdent(x syntax.Expr) (string, error) {
	switch i := x.(type) {
	case *syntax.Ident:
		return i.Name, nil
	default:
		return "", fmt.Errorf("expected identifier, got %v of type %T", i, i)
	}
}

// ExpectLiteralString returns the string represented by this Expr or an error
// if the Expr is not a literal string.
func ExpectLiteralString(x syntax.Expr) (string, error) {
	switch l := x.(type) {
	case *syntax.Literal:
		switch s := l.Value.(type) {
		case string:
			return s, nil
		default:
			return "", fmt.Errorf("expected literal string, got %v of type %T", s, s)
		}
	default:
		return "", fmt.Errorf("expected literal string, got %v of type %T", l, l)
	}
}

// ExpectSingletonStringList returns the string in this list or an error if this
// Expr is not a string list of length 1.
func ExpectSingletonStringList(x syntax.Expr) (string, error) {
	switch l := x.(type) {
	case *syntax.ListExpr:
		if len(l.List) != 1 {
			return "", fmt.Errorf("expected list to have one item, got %d in %v", len(l.List), l)
		}
		return ExpectLiteralString(l.List[0])
	default:
		return "", fmt.Errorf("expected list of strings, got %v of type %T", l, l)
	}
}

// ExpectTupleOfStrings returns the strings in the tuple represented by this
// Expr or an error if this is not a string tuple of the correct length.
func ExpectTupleOfStrings(x syntax.Expr, length int) ([]string, error) {
	for {
		switch t := x.(type) {
		case *syntax.ParenExpr:
			x = t.X
			continue
		case *syntax.TupleExpr:
			if len(t.List) != length {
				return nil, fmt.Errorf("expected tuple to have %d item, got %d in %v", length, len(t.List), t)
			}
			ret := make([]string, 0, len(t.List))
			for _, sub := range t.List {
				s, err := ExpectLiteralString(sub)
				if err != nil {
					return nil, err
				}
				ret = append(ret, s)
			}
			return ret, nil
		default:
			return nil, fmt.Errorf("expected tuple of strings, got %v of type %T", t, t)
		}
	}
}
