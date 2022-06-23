package zglob

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"

	"github.com/mattn/go-zglob/fastwalk"
)

var (
	envre = regexp.MustCompile(`^(\$[a-zA-Z][a-zA-Z0-9_]+|\$\([a-zA-Z][a-zA-Z0-9_]+\))$`)
	mu    sync.Mutex
)

type zenv struct {
	dre     *regexp.Regexp
	fre     *regexp.Regexp
	pattern string
	root    string
}

func New(pattern string) (*zenv, error) {
	globmask := ""
	root := ""
	for n, i := range strings.Split(filepath.ToSlash(pattern), "/") {
		if root == "" && (strings.Index(i, "*") != -1 || strings.Index(i, "{") != -1) {
			if globmask == "" {
				root = "."
			} else {
				root = filepath.ToSlash(globmask)
			}
		}
		if n == 0 && i == "~" {
			if runtime.GOOS == "windows" {
				i = os.Getenv("USERPROFILE")
			} else {
				i = os.Getenv("HOME")
			}
		}
		if envre.MatchString(i) {
			i = strings.Trim(strings.Trim(os.Getenv(i[1:]), "()"), `"`)
		}

		globmask = filepath.Join(globmask, i)
		if n == 0 {
			if runtime.GOOS == "windows" && filepath.VolumeName(i) != "" {
				globmask = i + "/"
			} else if len(globmask) == 0 {
				globmask = "/"
			}
		}
	}
	if root == "" {
		return &zenv{
			dre:     nil,
			fre:     nil,
			pattern: pattern,
			root:    "",
		}, nil
	}
	if globmask == "" {
		globmask = "."
	}
	globmask = filepath.ToSlash(filepath.Clean(globmask))

	cc := []rune(globmask)
	dirmask := ""
	filemask := ""
	for i := 0; i < len(cc); i++ {
		if cc[i] == '*' {
			if i < len(cc)-2 && cc[i+1] == '*' && cc[i+2] == '/' {
				filemask += "(.*/)?"
				if dirmask == "" {
					dirmask = filemask
				}
				i += 2
			} else {
				filemask += "[^/]*"
			}
		} else {
			if cc[i] == '{' {
				pattern := ""
				for j := i + 1; j < len(cc); j++ {
					if cc[j] == ',' {
						pattern += "|"
					} else if cc[j] == '}' {
						i = j
						break
					} else {
						c := cc[j]
						if c == '/' || ('0' <= c && c <= '9') || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || 255 < c {
							pattern += string(c)
						} else {
							pattern += fmt.Sprintf("[\\x%02X]", c)
						}
					}
				}
				if pattern != "" {
					filemask += "(" + pattern + ")"
					continue
				}
			}
			c := cc[i]
			if c == '/' || ('0' <= c && c <= '9') || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || 255 < c {
				filemask += string(c)
			} else {
				filemask += fmt.Sprintf("[\\x%02X]", c)
			}
			if c == '/' && dirmask == "" && strings.Index(filemask, "*") != -1 {
				dirmask = filemask
			}
		}
	}
	if dirmask == "" {
		dirmask = filemask
	}
	if len(filemask) > 0 && filemask[len(filemask)-1] == '/' {
		if root == "" {
			root = filemask
		}
		filemask += "[^/]*"
	}
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		dirmask = "(?i:" + dirmask + ")"
		filemask = "(?i:" + filemask + ")"
	}
	return &zenv{
		dre:     regexp.MustCompile("^" + dirmask),
		fre:     regexp.MustCompile("^" + filemask + "$"),
		pattern: pattern,
		root:    filepath.Clean(root),
	}, nil
}

func Glob(pattern string) ([]string, error) {
	return glob(pattern, false)
}

func GlobFollowSymlinks(pattern string) ([]string, error) {
	return glob(pattern, true)
}

func glob(pattern string, followSymlinks bool) ([]string, error) {
	zenv, err := New(pattern)
	if err != nil {
		return nil, err
	}
	if zenv.root == "" {
		_, err := os.Stat(pattern)
		if err != nil {
			return nil, os.ErrNotExist
		}
		return []string{pattern}, nil
	}
	relative := !filepath.IsAbs(pattern)
	matches := []string{}

	fastwalk.FastWalk(zenv.root, func(path string, info os.FileMode) error {
		if zenv.root == "." && len(zenv.root) < len(path) {
			path = path[len(zenv.root)+1:]
		}
		path = filepath.ToSlash(path)

		if followSymlinks && info == os.ModeSymlink {
			followedPath, err := filepath.EvalSymlinks(path)
			if err == nil {
				fi, err := os.Lstat(followedPath)
				if err == nil && fi.IsDir() {
					return fastwalk.TraverseLink
				}
			}
		}

		if info.IsDir() {
			if path == "." || len(path) <= len(zenv.root) {
				return nil
			}
			if zenv.fre.MatchString(path) {
				mu.Lock()
				matches = append(matches, path)
				mu.Unlock()
				return nil
			}
			if !zenv.dre.MatchString(path + "/") {
				return filepath.SkipDir
			}
		}

		if zenv.fre.MatchString(path) {
			if relative && filepath.IsAbs(path) {
				path = path[len(zenv.root)+1:]
			}
			mu.Lock()
			matches = append(matches, path)
			mu.Unlock()
		}
		return nil
	})
	return matches, nil
}

func Match(pattern, name string) (matched bool, err error) {
	zenv, err := New(pattern)
	if err != nil {
		return false, err
	}
	return zenv.Match(name), nil
}

func (z *zenv) Match(name string) bool {
	if z.root == "" {
		return z.pattern == name
	}

	name = filepath.ToSlash(name)

	if name == "." || len(name) <= len(z.root) {
		return false
	}

	if z.fre.MatchString(name) {
		return true
	}
	return false
}
