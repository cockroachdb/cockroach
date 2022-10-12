package jwx

import (
	"bytes"
	"encoding/json"
)

type FormatKind int

const (
	UnknownFormat FormatKind = iota
	JWE
	JWS
	JWK
	JWKS
	JWT
)

type formatHint struct {
	Payload    json.RawMessage `json:"payload"`    // Only in JWS
	Signatures json.RawMessage `json:"signatures"` // Only in JWS
	Ciphertext json.RawMessage `json:"ciphertext"` // Only in JWE
	KeyType    json.RawMessage `json:"kty"`        // Only in JWK
	Keys       json.RawMessage `json:"keys"`       // Only in JWKS
	Audience   json.RawMessage `json:"aud"`        // Only in JWT
}

// GuessFormat is used to guess the format the given payload is in
// using heuristics. See the type FormatKind for a full list of
// possible types.
//
// This may be useful in determining your next action when you may
// encounter a payload that could either be a JWE, JWS, or a plain JWT.
//
// Because JWTs are almost always JWS signed, you may be thrown off
// if you pass what you think is a JWT payload to this function.
// If the function is in the "Compact" format, it means it's a JWS
// signed message, and its payload is the JWT. Therefore this function
// will reuturn JWS, not JWT.
//
// This function requires an extra parsing of the payload, and therefore
// may be inefficient if you call it every time before parsing.
func GuessFormat(payload []byte) FormatKind {
	// The check against kty, keys, and aud are something this library
	// made up. for the distinctions between JWE and JWS, we used
	// https://datatracker.ietf.org/doc/html/rfc7516#section-9.
	//
	// The above RFC described several ways to distinguish between
	// a JWE and JWS JSON, but we're only using one of them

	payload = bytes.TrimSpace(payload)
	if len(payload) <= 0 {
		return UnknownFormat
	}

	if payload[0] != '{' {
		// Compact format. It's probably a JWS or JWE
		sep := []byte{'.'} // I want to const this :/

		// Note: this counts the number of occurrences of the
		// separator, but the RFC talks about the number of segments.
		// number of '.' == segments - 1, so that's why we have 2 and 4 here
		switch count := bytes.Count(payload, sep); count {
		case 2:
			return JWS
		case 4:
			return JWE
		default:
			return UnknownFormat
		}
	}

	// If we got here, we probably have JSON.
	var h formatHint
	if err := json.Unmarshal(payload, &h); err != nil {
		return UnknownFormat
	}

	if h.Audience != nil {
		return JWT
	}
	if h.KeyType != nil {
		return JWK
	}
	if h.Keys != nil {
		return JWKS
	}
	if h.Ciphertext != nil {
		return JWE
	}
	if h.Signatures != nil && h.Payload != nil {
		return JWS
	}
	return UnknownFormat
}
