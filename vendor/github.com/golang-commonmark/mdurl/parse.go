// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mdurl

import (
	"errors"
	"strings"
)

// ErrMissingScheme error is returned by Parse if the passed URL starts with a colon.
var ErrMissingScheme = errors.New("missing protocol scheme")

var slashedProtocol = map[string]bool{
	"http":   true,
	"https":  true,
	"ftp":    true,
	"gopher": true,
	"file":   true,
}

func split(s string, c byte) (string, string, bool) {
	i := strings.IndexByte(s, c)
	if i < 0 {
		return s, "", false
	}
	return s[:i], s[i+1:], true
}

func findScheme(s string) (int, error) {
	if s == "" {
		return 0, nil
	}

	b := s[0]
	if b == ':' {
		return 0, ErrMissingScheme
	}
	if !letter(b) {
		return 0, nil
	}

	for i := 1; i < len(s); i++ {
		b := s[i]
		switch {
		case b == ':':
			return i, nil
		case strings.IndexByte("+-.0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", b) != -1:
			// do nothing
		default:
			return 0, nil
		}
	}

	return 0, nil
}

// Parse parses rawurl into a URL structure.
func Parse(rawurl string) (*URL, error) {
	n, err := findScheme(rawurl)
	if err != nil {
		return nil, err
	}

	var url URL
	rest := rawurl
	hostless := false
	if n > 0 {
		url.RawScheme = rest[:n]
		url.Scheme, rest = strings.ToLower(rest[:n]), rest[n+1:]
		if url.Scheme == "javascript" {
			hostless = true
		}
	}

	if !hostless && strings.HasPrefix(rest, "//") {
		url.Slashes, rest = true, rest[2:]
	}

	if !hostless && (url.Slashes || (url.Scheme != "" && !slashedProtocol[url.Scheme])) {
		hostEnd := strings.IndexAny(rest, "#/?")
		atSign := -1
		i := hostEnd
		if i == -1 {
			i = len(rest) - 1
		}
		for i >= 0 {
			if rest[i] == '@' {
				atSign = i
				break
			}
			i--
		}

		if atSign != -1 {
			url.Auth, rest = rest[:atSign], rest[atSign+1:]
		}

		hostEnd = strings.IndexAny(rest, "\t\r\n \"#%'/;<>?\\^`{|}")
		if hostEnd == -1 {
			hostEnd = len(rest)
		}
		if hostEnd > 0 && hostEnd < len(rest) && rest[hostEnd-1] == ':' {
			hostEnd--
		}
		host := rest[:hostEnd]

		if len(host) > 1 {
			b := host[hostEnd-1]
			if digit(b) {
				for i := len(host) - 2; i >= 0; i-- {
					b := host[i]
					if b == ':' {
						url.Host, url.Port = host[:i], host[i+1:]
						break
					}
					if !digit(b) {
						break
					}
				}
			} else if b == ':' {
				host = host[:hostEnd-1]
				hostEnd--
			}
		}
		if url.Port == "" {
			url.Host = host
		}
		rest = rest[hostEnd:]

		if ipv6 := len(url.Host) > 2 &&
			url.Host[0] == '[' &&
			url.Host[len(url.Host)-1] == ']'; ipv6 {
			url.Host = url.Host[1 : len(url.Host)-1]
			url.IPv6 = true
		} else if i := strings.IndexByte(url.Host, ':'); i >= 0 {
			url.Host, rest = url.Host[:i], url.Host[i:]+rest
		}
	}

	rest, url.Fragment, url.HasFragment = split(rest, '#')
	url.Path, url.RawQuery, url.HasQuery = split(rest, '?')

	return &url, nil
}
