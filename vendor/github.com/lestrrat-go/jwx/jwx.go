//go:generate ./gen.sh
//go:generate stringer -type=FormatKind
//go:generate mv formatkind_string.go formatkind_string_gen.go

// Package jwx contains tools that deal with the various JWx (JOSE)
// technologies such as JWT, JWS, JWE, etc in Go.
//
//    JWS (https://tools.ietf.org/html/rfc7515)
//    JWE (https://tools.ietf.org/html/rfc7516)
//    JWK (https://tools.ietf.org/html/rfc7517)
//    JWA (https://tools.ietf.org/html/rfc7518)
//    JWT (https://tools.ietf.org/html/rfc7519)
//
// Examples are stored in a separate Go module (to avoid adding
// dependencies to this module), and thus does not appear in the
// online documentation for this module.
// You can find the examples in Github at https://github.com/lestrrat-go/jwx/examples
//
// You can find more high level documentation at Github (https://github.com/lestrrat-go/jwx)
//
// FAQ style documentation can be found in the repository (https://github.com/lestrrat-go/jwx/tree/develop/v2/docs)
package jwx

import (
	"github.com/lestrrat-go/jwx/internal/json"
)

// DecoderSettings gives you a access to configure the "encoding/json".Decoder
// used to decode JSON objects within the jwx framework.
func DecoderSettings(options ...JSONOption) {
	// XXX We're using this format instead of just passing a single boolean
	// in case a new option is to be added some time later
	var useNumber bool
	for _, option := range options {
		//nolint:forcetypeassert
		switch option.Ident() {
		case identUseNumber{}:
			useNumber = option.Value().(bool)
		}
	}

	json.DecoderSettings(useNumber)
}
