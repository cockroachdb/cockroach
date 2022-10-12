package httpcc

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	// Request Cache-Control directives
	MaxAge       = "max-age" // used in response as well
	MaxStale     = "max-stale"
	MinFresh     = "min-fresh"
	NoCache      = "no-cache"     // used in response as well
	NoStore      = "no-store"     // used in response as well
	NoTransform  = "no-transform" // used in response as well
	OnlyIfCached = "only-if-cached"

	// Response Cache-Control directive
	MustRevalidate  = "must-revalidate"
	Public          = "public"
	Private         = "private"
	ProxyRevalidate = "proxy-revalidate"
	SMaxAge         = "s-maxage"
)

type TokenPair struct {
	Name  string
	Value string
}

type TokenValuePolicy int

const (
	NoArgument TokenValuePolicy = iota
	TokenOnly
	QuotedStringOnly
	AnyTokenValue
)

type directiveValidator interface {
	Validate(string) TokenValuePolicy
}
type directiveValidatorFn func(string) TokenValuePolicy

func (fn directiveValidatorFn) Validate(ccd string) TokenValuePolicy {
	return fn(ccd)
}

func responseDirectiveValidator(s string) TokenValuePolicy {
	switch s {
	case MustRevalidate, NoStore, NoTransform, Public, ProxyRevalidate:
		return NoArgument
	case NoCache, Private:
		return QuotedStringOnly
	case MaxAge, SMaxAge:
		return TokenOnly
	default:
		return AnyTokenValue
	}
}

func requestDirectiveValidator(s string) TokenValuePolicy {
	switch s {
	case MaxAge, MaxStale, MinFresh:
		return TokenOnly
	case NoCache, NoStore, NoTransform, OnlyIfCached:
		return NoArgument
	default:
		return AnyTokenValue
	}
}

// ParseRequestDirective parses a single token.
func ParseRequestDirective(s string) (*TokenPair, error) {
	return parseDirective(s, directiveValidatorFn(requestDirectiveValidator))
}

func ParseResponseDirective(s string) (*TokenPair, error) {
	return parseDirective(s, directiveValidatorFn(responseDirectiveValidator))
}

func parseDirective(s string, ccd directiveValidator) (*TokenPair, error) {
	s = strings.TrimSpace(s)

	i := strings.IndexByte(s, '=')
	if i == -1 {
		return &TokenPair{Name: s}, nil
	}

	pair := &TokenPair{Name: strings.TrimSpace(s[:i])}

	if len(s) <= i {
		// `key=` feels like it's a parse error, but it's HTTP...
		// for now, return as if nothing happened.
		return pair, nil
	}

	v := strings.TrimSpace(s[i+1:])
	switch ccd.Validate(pair.Name) {
	case TokenOnly:
		if v[0] == '"' {
			return nil, fmt.Errorf(`invalid value for %s (quoted string not allowed)`, pair.Name)
		}
	case QuotedStringOnly: // quoted-string only
		if v[0] != '"' {
			return nil, fmt.Errorf(`invalid value for %s (bare token not allowed)`, pair.Name)
		}
		tmp, err := strconv.Unquote(v)
		if err != nil {
			return nil, fmt.Errorf(`malformed quoted string in token`)
		}
		v = tmp
	case AnyTokenValue:
		if v[0] == '"' {
			tmp, err := strconv.Unquote(v)
			if err != nil {
				return nil, fmt.Errorf(`malformed quoted string in token`)
			}
			v = tmp
		}
	case NoArgument:
		if len(v) > 0 {
			return nil, fmt.Errorf(`received argument to directive %s`, pair.Name)
		}
	}

	pair.Value = v
	return pair, nil
}

func ParseResponseDirectives(s string) ([]*TokenPair, error) {
	return parseDirectives(s, ParseResponseDirective)
}

func ParseRequestDirectives(s string) ([]*TokenPair, error) {
	return parseDirectives(s, ParseRequestDirective)
}

func parseDirectives(s string, p func(string) (*TokenPair, error)) ([]*TokenPair, error) {
	scanner := bufio.NewScanner(strings.NewReader(s))
	scanner.Split(scanCommaSeparatedWords)

	var tokens []*TokenPair
	for scanner.Scan() {
		tok, err := p(scanner.Text())
		if err != nil {
			return nil, fmt.Errorf(`failed to parse token #%d: %w`, len(tokens)+1, err)
		}
		tokens = append(tokens, tok)
	}
	return tokens, nil
}

// isSpace reports whether the character is a Unicode white space character.
// We avoid dependency on the unicode package, but check validity of the implementation
// in the tests.
func isSpace(r rune) bool {
	if r <= '\u00FF' {
		// Obvious ASCII ones: \t through \r plus space. Plus two Latin-1 oddballs.
		switch r {
		case ' ', '\t', '\n', '\v', '\f', '\r':
			return true
		case '\u0085', '\u00A0':
			return true
		}
		return false
	}
	// High-valued ones.
	if '\u2000' <= r && r <= '\u200a' {
		return true
	}
	switch r {
	case '\u1680', '\u2028', '\u2029', '\u202f', '\u205f', '\u3000':
		return true
	}
	return false
}

func scanCommaSeparatedWords(data []byte, atEOF bool) (advance int, token []byte, err error) {
	// Skip leading spaces.
	start := 0
	for width := 0; start < len(data); start += width {
		var r rune
		r, width = utf8.DecodeRune(data[start:])
		if !isSpace(r) {
			break
		}
	}
	// Scan until we find a comma. Keep track of consecutive whitespaces
	// so we remove them from the end result
	var ws int
	for width, i := 0, start; i < len(data); i += width {
		var r rune
		r, width = utf8.DecodeRune(data[i:])
		switch {
		case isSpace(r):
			ws++
		case r == ',':
			return i + width, data[start : i-ws], nil
		default:
			ws = 0
		}
	}

	// If we're at EOF, we have a final, non-empty, non-terminated word. Return it.
	if atEOF && len(data) > start {
		return len(data), data[start : len(data)-ws], nil
	}

	// Request more data.
	return start, nil, nil
}

// ParseRequest parses the content of `Cache-Control` header of an HTTP Request.
func ParseRequest(v string) (*RequestDirective, error) {
	var dir RequestDirective
	tokens, err := ParseRequestDirectives(v)
	if err != nil {
		return nil, fmt.Errorf(`failed to parse tokens: %w`, err)
	}

	for _, token := range tokens {
		name := strings.ToLower(token.Name)
		switch name {
		case MaxAge:
			iv, err := strconv.ParseUint(token.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf(`failed to parse max-age: %w`, err)
			}
			dir.maxAge = &iv
		case MaxStale:
			iv, err := strconv.ParseUint(token.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf(`failed to parse max-stale: %w`, err)
			}
			dir.maxStale = &iv
		case MinFresh:
			iv, err := strconv.ParseUint(token.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf(`failed to parse min-fresh: %w`, err)
			}
			dir.minFresh = &iv
		case NoCache:
			dir.noCache = true
		case NoStore:
			dir.noStore = true
		case NoTransform:
			dir.noTransform = true
		case OnlyIfCached:
			dir.onlyIfCached = true
		default:
			dir.extensions[token.Name] = token.Value
		}
	}
	return &dir, nil
}

// ParseResponse parses the content of `Cache-Control` header of an HTTP Response.
func ParseResponse(v string) (*ResponseDirective, error) {
	tokens, err := ParseResponseDirectives(v)
	if err != nil {
		return nil, fmt.Errorf(`failed to parse tokens: %w`, err)
	}

	var dir ResponseDirective
	dir.extensions = make(map[string]string)
	for _, token := range tokens {
		name := strings.ToLower(token.Name)
		switch name {
		case MaxAge:
			iv, err := strconv.ParseUint(token.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf(`failed to parse max-age: %w`, err)
			}
			dir.maxAge = &iv
		case NoCache:
			scanner := bufio.NewScanner(strings.NewReader(token.Value))
			scanner.Split(scanCommaSeparatedWords)
			for scanner.Scan() {
				dir.noCache = append(dir.noCache, scanner.Text())
			}
		case NoStore:
			dir.noStore = true
		case NoTransform:
			dir.noTransform = true
		case Public:
			dir.public = true
		case Private:
			scanner := bufio.NewScanner(strings.NewReader(token.Value))
			scanner.Split(scanCommaSeparatedWords)
			for scanner.Scan() {
				dir.private = append(dir.private, scanner.Text())
			}
		case ProxyRevalidate:
			dir.proxyRevalidate = true
		case SMaxAge:
			iv, err := strconv.ParseUint(token.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf(`failed to parse s-maxage: %w`, err)
			}
			dir.sMaxAge = &iv
		default:
			dir.extensions[token.Name] = token.Value
		}
	}
	return &dir, nil
}
