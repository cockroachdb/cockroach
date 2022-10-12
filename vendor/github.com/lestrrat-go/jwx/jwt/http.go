package jwt

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/lestrrat-go/jwx/internal/pool"
	"github.com/pkg/errors"
)

// ParseHeader parses a JWT stored in a http.Header.
//
// For the header "Authorization", it will strip the prefix "Bearer " and will
// treat the remaining value as a JWT.
func ParseHeader(hdr http.Header, name string, options ...ParseOption) (Token, error) {
	key := http.CanonicalHeaderKey(name)
	v := strings.TrimSpace(hdr.Get(key))
	if v == "" {
		return nil, errors.Errorf(`empty header (%s)`, key)
	}

	if key == "Authorization" {
		// Authorization header is an exception. We strip the "Bearer " from
		// the prefix
		v = strings.TrimSpace(strings.TrimPrefix(v, "Bearer"))
	}

	return ParseString(v, options...)
}

// ParseForm parses a JWT stored in a url.Value.
func ParseForm(values url.Values, name string, options ...ParseOption) (Token, error) {
	v := strings.TrimSpace(values.Get(name))
	if v == "" {
		return nil, errors.Errorf(`empty value (%s)`, name)
	}

	return ParseString(v, options...)
}

// ParseRequest searches a http.Request object for a JWT token.
//
// Specifying WithHeaderKey() will tell it to search under a specific
// header key. Specifying WithFormKey() will tell it to search under
// a specific form field.
//
// By default, "Authorization" header will be searched.
//
// If WithHeaderKey() is used, you must explicitly re-enable searching for "Authorization" header.
//
//   # searches for "Authorization"
//   jwt.ParseRequest(req)
//
//   # searches for "x-my-token" ONLY.
//   jwt.ParseRequest(req, jwt.WithHeaderKey("x-my-token"))
//
//   # searches for "Authorization" AND "x-my-token"
//   jwt.ParseRequest(req, jwt.WithHeaderKey("Authorization"), jwt.WithHeaderKey("x-my-token"))
func ParseRequest(req *http.Request, options ...ParseOption) (Token, error) {
	var hdrkeys []string
	var formkeys []string
	var parseOptions []ParseOption
	for _, option := range options {
		//nolint:forcetypeassert
		switch option.Ident() {
		case identHeaderKey{}:
			hdrkeys = append(hdrkeys, option.Value().(string))
		case identFormKey{}:
			formkeys = append(formkeys, option.Value().(string))
		default:
			parseOptions = append(parseOptions, option)
		}
	}
	if len(hdrkeys) == 0 {
		hdrkeys = append(hdrkeys, "Authorization")
	}

	mhdrs := pool.GetKeyToErrorMap()
	defer pool.ReleaseKeyToErrorMap(mhdrs)
	mfrms := pool.GetKeyToErrorMap()
	defer pool.ReleaseKeyToErrorMap(mfrms)

	for _, hdrkey := range hdrkeys {
		// Check presence via a direct map lookup
		if _, ok := req.Header[http.CanonicalHeaderKey(hdrkey)]; !ok {
			// if non-existent, not error
			continue
		}

		tok, err := ParseHeader(req.Header, hdrkey, parseOptions...)
		if err != nil {
			mhdrs[hdrkey] = err
			continue
		}
		return tok, nil
	}

	if cl := req.ContentLength; cl > 0 {
		if err := req.ParseForm(); err != nil {
			return nil, errors.Wrap(err, `failed to parse form`)
		}
	}

	for _, formkey := range formkeys {
		// Check presence via a direct map lookup
		if _, ok := req.Form[formkey]; !ok {
			// if non-existent, not error
			continue
		}

		tok, err := ParseForm(req.Form, formkey, parseOptions...)
		if err != nil {
			mfrms[formkey] = err
			continue
		}
		return tok, nil
	}

	// Everything below is a preulde to error reporting.
	var triedHdrs strings.Builder
	for i, hdrkey := range hdrkeys {
		if i > 0 {
			triedHdrs.WriteString(", ")
		}
		triedHdrs.WriteString(strconv.Quote(hdrkey))
	}

	var triedForms strings.Builder
	for i, formkey := range formkeys {
		if i > 0 {
			triedForms.WriteString(", ")
		}
		triedForms.WriteString(strconv.Quote(formkey))
	}

	var b strings.Builder
	b.WriteString(`failed to find a valid token in any location of the request (tried: [header keys: `)
	b.WriteString(triedHdrs.String())
	b.WriteByte(']')
	if triedForms.Len() > 0 {
		b.WriteString(", form keys: [")
		b.WriteString(triedForms.String())
		b.WriteByte(']')
	}
	b.WriteByte(')')

	lmhdrs := len(mhdrs)
	lmfrms := len(mfrms)
	if lmhdrs > 0 || lmfrms > 0 {
		b.WriteString(". Additionally, errors were encountered during attempts to parse")

		if lmhdrs > 0 {
			b.WriteString(" headers: (")
			count := 0
			for hdrkey, err := range mhdrs {
				if count > 0 {
					b.WriteString(", ")
				}
				b.WriteString("[header key: ")
				b.WriteString(strconv.Quote(hdrkey))
				b.WriteString(", error: ")
				b.WriteString(strconv.Quote(err.Error()))
				b.WriteString("]")
				count++
			}
			b.WriteString(")")
		}

		if lmfrms > 0 {
			count := 0
			b.WriteString(" forms: (")
			for formkey, err := range mfrms {
				if count > 0 {
					b.WriteString(", ")
				}
				b.WriteString("[form key: ")
				b.WriteString(strconv.Quote(formkey))
				b.WriteString(", error: ")
				b.WriteString(strconv.Quote(err.Error()))
				b.WriteString("]")
				count++
			}
		}
	}
	return nil, errors.New(b.String())
}
