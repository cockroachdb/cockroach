// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security

import (
	"crypto/tls"
	"net"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/slices"
)

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

var tlsCipherSuitesConfigured struct {
	syncutil.RWMutex
	c []string
}

// getAllowedCiphersMapByName returns map of cipher names to cipher ID.
// These are all the ciphers which golang implements and have been allowed for
// cockroach in RecommendedCipherSuites, OldCipherSuites or as part of TLS 1.3
// ciphers in crypto/tls.
func getAllowedCiphersMapByName() map[string]uint16 {
	cockroachEnabledCiphers := append(RecommendedCipherSuites(), OldCipherSuites()...)
	ciphersMap := map[string]uint16{}
	for _, cipher := range tls.CipherSuites() {
		if slices.Contains(cockroachEnabledCiphers, cipher.ID) || slices.Contains(cipher.SupportedVersions, tls.VersionTLS13) {
			ciphersMap[cipher.Name] = cipher.ID
		}
	}
	for _, cipher := range tls.InsecureCipherSuites() {
		if slices.Contains(cockroachEnabledCiphers, cipher.ID) || slices.Contains(cipher.SupportedVersions, tls.VersionTLS13) {
			ciphersMap[cipher.Name] = cipher.ID
		}
	}

	return ciphersMap
}

// getAllowedCiphersMapByID returns map of cipher ID to cipher names.
// These are all the ciphers which golang implements and have been allowed for
// cockroach in RecommendedCipherSuites, OldCipherSuites or as part of TLS 1.3
// ciphers in crypto/tls.
func getAllowedCiphersMapByID() map[uint16]string {
	cockroachEnabledCiphers := append(RecommendedCipherSuites(), OldCipherSuites()...)
	ciphersMap := map[uint16]string{}
	for _, cipher := range tls.CipherSuites() {
		if slices.Contains(cockroachEnabledCiphers, cipher.ID) || slices.Contains(cipher.SupportedVersions, tls.VersionTLS13) {
			ciphersMap[cipher.ID] = cipher.Name
		}
	}
	for _, cipher := range tls.InsecureCipherSuites() {
		if slices.Contains(cockroachEnabledCiphers, cipher.ID) || slices.Contains(cipher.SupportedVersions, tls.VersionTLS13) {
			ciphersMap[cipher.ID] = cipher.Name
		}
	}

	return ciphersMap
}

// getCipherID verifies if provided cipher is implemented by crypto/tls and
// return the corresponding cipherID
func getCipherNameFromID(cid uint16) (cipher string, err error) {
	allowedCiphersMap := getAllowedCiphersMapByID()
	if cipher, ok := allowedCiphersMap[cid]; !ok {
		return cipher, errors.Errorf("cipher id %v does match implemented tls ciphers", cid)
	}
	return
}

// SetTLSCipherSuitesConfigured sets the global TLS cipher suites for all
// incoming connections(sql/rpc/http, etc.) of a node. The entries in the list
// should be a subset of RecommendedCipherSuites or OldCipherSuites in case of
// TLS 1.2. For TLS 1.3, they should be a subset of ciphers list defined at
// https://github.com/golang/go/blob/4aa1efed4853ea067d665a952eee77c52faac774/src/crypto/tls/cipher_suites.go#L676-L679
// for TLS 1.3.
func SetTLSCipherSuitesConfigured(ciphers []string) error {
	allowedCiphersMap := getAllowedCiphersMapByName()
	for _, cipher := range ciphers {
		if _, ok := allowedCiphersMap[cipher]; !ok {
			return errors.Errorf("invalid cipher provided in tls cipher suites: %s", cipher)
		}
	}

	tlsCipherSuitesConfigured.Lock()
	defer tlsCipherSuitesConfigured.Unlock()
	tlsCipherSuitesConfigured.c = ciphers

	return nil
}

func GetTLSCipherSuitesConfigured() []string {
	tlsCipherSuitesConfigured.Lock()
	defer tlsCipherSuitesConfigured.Unlock()
	return tlsCipherSuitesConfigured.c
}

// TLSCipherRestrict restricts the cipher suites used for tls connections to
// ones specified by tls-cipher-suites cli flag. If the flag is not set, we do
// not check for used ciphers in the connection. It returns an error if the used
// cipher is not present in the configured ciphers for the node.
func TLSCipherRestrict(conn net.Conn) (err error) {
	if configuredCiphers := GetTLSCipherSuitesConfigured(); len(configuredCiphers) != 0 {
		tlsError := errors.New("connection provided is not TLS, cannot obtain TLS connection details.")
		if tlsConn, ok := conn.(*tls.Conn); ok {
			selectedCipherID := tlsConn.ConnectionState().CipherSuite
			cName, err := getCipherNameFromID(selectedCipherID)
			if err != nil {
				return err
			}
			if !slices.Contains(configuredCiphers, cName) {
				return errors.Newf("presented cipher %s not in allowed cipher suite list", cName)
			} else {
				return tlsError
			}
		} else {
			return tlsError
		}
	}
	return
}
