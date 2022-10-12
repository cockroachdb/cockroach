package jwk

import "regexp"

// InsecureWhitelist allows any URLs to be fetched. This is the default
// behavior of `jwk.Fetch()`, but this exists to allow other libraries
// (such as jws, via jws.VerifyAuto) and users to be able to explicitly
// state that they intend to not check the URLs that are being fetched
type InsecureWhitelist struct{}

func (InsecureWhitelist) IsAllowed(string) bool {
	return true
}

// RegexpWhitelist is a jwk.Whitelist object comprised of a list of *regexp.Regexp
// objects. All entries in the list are tried until one matches. If none of the
// *regexp.Regexp objects match, then the URL is deemed unallowed.
type RegexpWhitelist struct {
	patterns []*regexp.Regexp
}

func NewRegexpWhitelist() *RegexpWhitelist {
	return &RegexpWhitelist{}
}

func (w *RegexpWhitelist) Add(pat *regexp.Regexp) *RegexpWhitelist {
	w.patterns = append(w.patterns, pat)
	return w
}

// IsAlloed returns true if any of the patterns in the whitelist
// returns true.
func (w *RegexpWhitelist) IsAllowed(u string) bool {
	for _, pat := range w.patterns {
		if pat.MatchString(u) {
			return true
		}
	}
	return false
}

// MapWhitelist is a jwk.Whitelist object comprised of a map of strings.
// If the URL exists in the map, then the URL is allowed to be fetched.
type MapWhitelist struct {
	store map[string]struct{}
}

func NewMapWhitelist() *MapWhitelist {
	return &MapWhitelist{store: make(map[string]struct{})}
}

func (w *MapWhitelist) Add(pat string) *MapWhitelist {
	w.store[pat] = struct{}{}
	return w
}

func (w *MapWhitelist) IsAllowed(u string) bool {
	_, b := w.store[u]
	return b
}

// WhitelistFunc is a jwk.Whitelist object based on a function.
// You can perform any sort of check against the given URL to determine
// if it can be fetched or not.
type WhitelistFunc func(string) bool

func (w WhitelistFunc) IsAllowed(u string) bool {
	return w(u)
}
