// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security

import "crypto/tls"

// RecommendedCipherSuites returns a list of enabled TLS 1.2 cipher
// suites. The order of the list is ignored; prioritization of cipher
// suites follows hard-coded rules in the Go standard library[1].
//
// Note that TLS 1.3 ciphersuites are not configurable.
//
// This is the subset of Go's default cipher suite list which are
// also marked as "recommended" by IETF[2] (As of June 1, 2022).
// Mozilla recommends the same list with some comments on
// rationale and compatibility[3]. These ciphers are recommended
// because they are the ones that provide forward secrecy and
// authenticated encryption (AEAD). Mozilla claims they are
// compatible with "nearly all" clients from the last five years.
//
// [1]: https://github.com/golang/go/blob/4aa1efed4853ea067d665a952eee77c52faac774/src/crypto/tls/cipher_suites.go#L215-L270
// [2]: https://www.iana.org/assignments/tls-parameters/tls-parameters.xhtml#tls-parameters-4
// [3]: https://wiki.mozilla.org/Security/Server_Side_TLS#Intermediate_compatibility_.28recommended.29
func RecommendedCipherSuites() []uint16 {
	return []uint16{
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
		// Note: the codec names
		// TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305
		// and
		// TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305
		// are merely aliases for the two above.
		//
		// NB: no need to add TLS 1.3 ciphers here. As per the
		// documentation of CipherSuites, the TLS 1.3 ciphers are not
		// configurable. Go's predefined list always applies. All TLS
		// 1.3 ciphers meet the forward secrecy and authenticated
		// encryption requirements mentioned above.
	}
}

// OldCipherSuites returns a list of "old" cipher suites for TLS v1.2,
// which adds back all ciphers from v22.1. These are enabled with the
// use of the COCKROACH_TLS_ENABLE_OLD_CIPHER_SUITES environment
// variable, and should strictly be used when the software
// CockroachDB is being used with cannot be upgraded.
//
// Cipher suites not included in the recommended lists referred to
// in the RecommendedCipherSuites documentation but required for
// backwards compatibility should be added here, so organizations
// can opt into using deprecated cipher suites rather than opting
// every CRDB cluster into a worse security stance.
func OldCipherSuites() []uint16 {
	return []uint16{
		tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
		tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_RSA_WITH_AES_128_CBC_SHA,
		tls.TLS_RSA_WITH_AES_256_CBC_SHA,
	}
}
