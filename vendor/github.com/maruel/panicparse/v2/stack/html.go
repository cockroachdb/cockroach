// Copyright 2017 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

//go:generate go run regen.go

package stack

import (
	"fmt"
	"html/template"
	"io"
	"log"
	"net/url"
	"regexp"
	"runtime"
	"strings"
	"time"
)

// ToHTML formats the aggregated buckets as HTML to the writer.
//
// Use footer to add custom HTML at the bottom of the page.
func (a *Aggregated) ToHTML(w io.Writer, footer template.HTML) error {
	data := map[string]interface{}{
		"Aggregated": a,
		"Footer":     footer,
		"Snapshot":   a.Snapshot,
	}
	return toHTML(w, data)
}

// ToHTML formats the snapshot as HTML to the writer.
//
// Use footer to add custom HTML at the bottom of the page.
func (s *Snapshot) ToHTML(w io.Writer, footer template.HTML) error {
	data := map[string]interface{}{
		"Footer":   footer,
		"Snapshot": s,
	}
	return toHTML(w, data)
}

// Private stuff.

func toHTML(w io.Writer, data map[string]interface{}) error {
	m := template.FuncMap{
		"funcClass": funcClass,
		"minus":     minus,
		"pkgURL":    pkgURL,
		"srcURL":    srcURL,
		"symbol":    symbol,
	}
	data["Favicon"] = favicon
	data["GOMAXPROCS"] = runtime.GOMAXPROCS(0)
	data["Now"] = time.Now().Truncate(time.Second)
	data["Version"] = runtime.Version()
	t, err := template.New("t").Funcs(m).Parse(indexHTML)
	if err != nil {
		return err
	}
	return t.Execute(w, data)
}

var reMethodSymbol = regexp.MustCompile(`^\(\*?([^)]+)\)(\..+)$`)

func funcClass(c *Call) template.HTML {
	if c.Func.IsPkgMain {
		return "FuncMain Exported"
	}
	s := c.Location.String()
	if c.Func.IsExported {
		s += " Exported"
	}
	return template.HTML("Func" + s)
}

func minus(i, j int) int {
	return i - j
}

// pkgURL returns a link to the godoc for the call.
func pkgURL(c *Call) template.URL {
	imp := c.ImportPath
	// Check for vendored code first.
	if i := strings.Index(imp, "/vendor/"); i != -1 {
		imp = imp[i+8:]
	}
	ip := escape(imp)
	if ip == "" {
		return ""
	}
	url := template.URL("")
	if c.Location == Stdlib {
		// This always links to the latest release, past releases are not online.
		// That's somewhat unfortunate.
		url = "https://golang.org/pkg/"
	} else {
		// TODO(maruel): Leverage Location.
		// Use pkg.go.dev when there's a version (go module) and godoc.org when
		// there's none (implies branch master).
		_, branch := getSrcBranchURL(c)
		if branch == "master" || branch == "" {
			url = "https://godoc.org/"
		} else {
			url = "https://pkg.go.dev/"
		}
	}
	if c.Func.IsExported {
		return template.URL(url + ip + "#" + symbol(&c.Func))
	}
	return template.URL(url + ip)
}

// srcURL returns an URL to the sources.
//
// TODO(maruel): Support custom local godoc server as it serves files too.
func srcURL(c *Call) template.URL {
	url, _ := getSrcBranchURL(c)
	return template.URL(url)
}

func escape(s string) template.URL {
	// That's the only way I found to get the kind of escaping I wanted, where
	// '/' is not escaped.
	u := url.URL{Path: s}
	return template.URL(u.EscapedPath())
}

// getSrcBranchURL returns a link to the source on the web and the tag name for
// the package version, if possible.
func getSrcBranchURL(c *Call) (template.URL, template.URL) {
	tag := ""
	if c.Location == Stdlib {
		// TODO(maruel): This is not strictly speaking correct. The remote could be
		// running a different Go version from the current executable.
		ver := runtime.Version()
		const devel = "devel +"
		if strings.HasPrefix(ver, devel) {
			ver = ver[len(devel) : len(devel)+10]
		}
		tag = url.QueryEscape(ver)
		return template.URL(fmt.Sprintf("https://github.com/golang/go/blob/%s/src/%s#L%d", tag, escape(c.RelSrcPath), c.Line)), template.URL(tag)
	}
	// TODO(maruel): Leverage Location.
	if rel := c.RelSrcPath; rel != "" {
		// Check for vendored code first.
		if i := strings.Index(rel, "/vendor/"); i != -1 {
			rel = rel[i+8:]
		}
		// Specialized support for github and golang.org. This will cover a fair
		// share of the URLs, but it'd be nice to support others too. Please submit
		// a PR (including a unit test that I was too lazy to add yet).
		switch host, rest := splitHost(rel); host {
		case "github.com":
			if parts := strings.SplitN(rest, "/", 3); len(parts) == 3 {
				p, srcTag, tag := splitTag(parts[1])
				url := fmt.Sprintf("https://github.com/%s/%s/blob/%s/%s#L%d", escape(parts[0]), p, srcTag, escape(parts[2]), c.Line)
				return template.URL(url), tag
			}
			log.Printf("problematic github.com URL: %q", rel)
		case "golang.org":
			// https://github.com/golang/build/blob/HEAD/repos/repos.go lists all
			// the golang.org/x/<foo> packages.
			if parts := strings.SplitN(rest, "/", 3); len(parts) == 3 && parts[0] == "x" {
				// parts is: "x", <project@version>, <path inside the repo>.
				p, srcTag, tag := splitTag(parts[1])
				// The source of truth is are actually go.googlesource.com, but
				// github.com has nicer syntax highlighting.
				url := fmt.Sprintf("https://github.com/golang/%s/blob/%s/%s#L%d", p, srcTag, escape(parts[2]), c.Line)
				return template.URL(url), tag
			}
			log.Printf("problematic golang.org URL: %q", rel)
		default:
			// For example gopkg.in. In this case there's no known way to find the
			// link to the source files, but we can still try to extract the version
			// if fetched from a go module.
			// Do a best effort to find a version by searching for a '@'.
			if i := strings.IndexByte(rel, '@'); i != -1 {
				if j := strings.IndexByte(rel[i:], '/'); j != -1 {
					tag = rel[i+1 : i+j]
				}
			}
		}
	}

	if c.LocalSrcPath != "" {
		return template.URL("file:///" + escape(c.LocalSrcPath)), template.URL(tag)
	}
	if c.RemoteSrcPath != "" {
		return template.URL("file:///" + escape(c.RemoteSrcPath)), template.URL(tag)
	}
	return "", ""
}

func splitHost(s string) (string, string) {
	parts := strings.SplitN(s, "/", 2)
	if len(parts) != 2 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}

// "v0.0.0-20200223170610-d5e6a3e2c0ae"
var reVersion = regexp.MustCompile(`v\d+\.\d+\.\d+\-\d+\-([a-f0-9]+)`)

func splitTag(s string) (string, string, template.URL) {
	// Default to branch master for non-versioned dependencies. It's not
	// optimal but it's better than nothing?
	// TODO(maruel): Replace with HEAD.
	i := strings.IndexByte(s, '@')
	if i == -1 {
		// No tag was found.
		return s, "master", "master"
	}
	// We got a versioned go module.
	tag := s[i+1:]
	srcTag := tag
	if m := reVersion.FindStringSubmatch(tag); len(m) != 0 {
		srcTag = m[1]
	}
	return s[:i], url.QueryEscape(srcTag), template.URL(url.QueryEscape(tag))
}

// symbol is the hashtag to use to refer to the symbol when looking at
// documentation.
//
// All of godoc/gddo, pkg.go.dev and golang.org/godoc use the same symbol
// reference format.
func symbol(f *Func) template.URL {
	s := f.Name
	if reMethodSymbol.MatchString(s) {
		// Transform the method form.
		s = reMethodSymbol.ReplaceAllString(s, "$1$2")
	}
	return template.URL(url.QueryEscape(s))
}
