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
	"github.com/cockroachdb/redact"
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

type tlsRestrictConfiguration struct {
	syncutil.RWMutex
	c          []string
	restrictFn func(*tls.Conn) (error net.Error)
}

var tlsRestrictConfig = tlsRestrictConfiguration{
	c:          []string{},
	restrictFn: func(tlsConn *tls.Conn) (error net.Error) { return },
}

type allowedTLSCiphers struct {
	ciphersMapByName map[string]uint16
	ciphersMapByID   map[uint16]string
}

// getAllowedCiphersMapByName returns map of allowed cipher names to cipher ID.
func (ciphers *allowedTLSCiphers) getAllowedCiphersMapByName() map[string]uint16 {
	return ciphers.ciphersMapByName
}

// getAllowedCiphersMapByID returns map of allowed cipher ID to cipher names.
func (ciphers *allowedTLSCiphers) getAllowedCiphersMapByID() map[uint16]string {
	return ciphers.ciphersMapByID
}

// newAllowedTLSCiphers instantiates allowedTLSCiphers with all the ciphers
// which golang implements and have been allowed for cockroach in
// RecommendedCipherSuites, OldCipherSuites or as part of TLS 1.3 ciphers in
// crypto/tls.
func newAllowedTLSCiphers() (ciphers *allowedTLSCiphers) {
	ciphers = &allowedTLSCiphers{}
	ciphers.ciphersMapByName = map[string]uint16{}
	ciphers.ciphersMapByID = map[uint16]string{}
	cockroachEnabledCiphers := append(RecommendedCipherSuites(), OldCipherSuites()...)
	for _, cipher := range tls.CipherSuites() {
		if slices.Contains(cockroachEnabledCiphers, cipher.ID) || slices.Contains(cipher.SupportedVersions, tls.VersionTLS13) {
			ciphers.ciphersMapByName[cipher.Name] = cipher.ID
			ciphers.ciphersMapByID[cipher.ID] = cipher.Name
		}
	}
	for _, cipher := range tls.InsecureCipherSuites() {
		if slices.Contains(cockroachEnabledCiphers, cipher.ID) || slices.Contains(cipher.SupportedVersions, tls.VersionTLS13) {
			ciphers.ciphersMapByName[cipher.Name] = cipher.ID
			ciphers.ciphersMapByID[cipher.ID] = cipher.Name
		}
	}
	return
}

var allowedCiphers = newAllowedTLSCiphers()

// getCipherID verifies if provided cipher is implemented by crypto/tls and
// return the corresponding cipherID
func getCipherNameFromID(cid uint16) (cipher string, ok bool) {
	allowedCiphersMap := allowedCiphers.getAllowedCiphersMapByID()
	cipher, ok = allowedCiphersMap[cid]
	return
}

// SetTLSCipherSuitesConfigured sets the global TLS cipher suites for all
// incoming connections(sql/rpc/http, etc.) of a node. The entries in the list
// should be a subset of RecommendedCipherSuites or OldCipherSuites in case of
// TLS 1.2. For TLS 1.3, they should be a subset of ciphers list defined at
// https://github.com/golang/go/blob/4aa1efed4853ea067d665a952eee77c52faac774/src/crypto/tls/cipher_suites.go#L676-L679
// for TLS 1.3.
func SetTLSCipherSuitesConfigured(ciphers []string) error {
	allowedCiphersMap := allowedCiphers.getAllowedCiphersMapByName()
	for _, cipher := range ciphers {
		if _, ok := allowedCiphersMap[cipher]; !ok {
			return &cipherRestrictError{errors.Errorf("invalid cipher provided in tls cipher suites: %s", cipher)}
		}
	}

	tlsRestrictConfig.configureTLSRestrict(ciphers)
	return nil
}

func (*tlsRestrictConfiguration) configureTLSRestrict(ciphers []string) {
	tlsRestrictConfig.Lock()
	defer tlsRestrictConfig.Unlock()
	tlsRestrictConfig.restrictFn = func(tlsConn *tls.Conn) (error net.Error) { return }
	tlsRestrictConfig.c = ciphers
	if len(ciphers) == 0 {
		return
	}

	tlsRestrictConfig.restrictFn = func(tlsConn *tls.Conn) (error net.Error) {
		if !tlsConn.ConnectionState().HandshakeComplete {
			// TODO(souravcrl): we need to provide a timebound context for handshake as it
			// ensures client failures are properly handled, issue: #144754
			if err := tlsConn.Handshake(); err != nil {
				// we don't want to close the connection for handshake errors
				return nil //nolint:returnerrcheck
			}
		}
		selectedCipherID := tlsConn.ConnectionState().CipherSuite
		cName, ok := getCipherNameFromID(selectedCipherID)
		if !ok {
			return &cipherRestrictError{errors.Errorf("cipher id %v does match implemented tls ciphers", selectedCipherID)}
		}
		if !slices.Contains(tlsRestrictConfig.c, cName) {
			return &cipherRestrictError{errors.Newf("presented cipher %s not in allowed cipher suite list", redact.SafeString(cName))}
		}
		return
	}
}

// TLSCipherRestrict restricts the cipher suites used for tls connections to
// ones specified by tls-cipher-suites cli flag. If the flag is not set, we do
// not check for used ciphers in the connection. It returns an error if the used
// cipher is not present in the configured ciphers for the node.
var TLSCipherRestrict = func(conn net.Conn) (err net.Error) {
	var tlsRestrictFn func(*tls.Conn) (error net.Error)
	{
		tlsRestrictConfig.Lock()
		defer tlsRestrictConfig.Unlock()
		tlsRestrictFn = tlsRestrictConfig.restrictFn
	}
	// we always expect a TLS connection here, since this is executed on the
	// tls.Listener or post applying tls.Server on the incoming connection
	tlsConn, _ := conn.(*tls.Conn)
	return tlsRestrictFn(tlsConn)
}

// cipherRestrictError implements net.Error interface so that we can override
// the error handling in net/http package as it only considers a net.Error type
// for error rule matching. The cipher restrict error is overridden by net.Error
// in TLSCipherRestrict fn.
type cipherRestrictError struct {
	err error
}

// Error implements net.Error.
func (e *cipherRestrictError) Error() string { return e.err.Error() }

// Unwrap implements net.Error.
func (e *cipherRestrictError) Unwrap() error { return e.err }

// Timeout implements net.Error.
func (e *cipherRestrictError) Timeout() bool { return false }

// Temporary implements net.Error. We need to set this to true since
// http/server.go:func Serve(l net.Listener) error
// https://github.com/golang/go/blob/go1.23.7/src/net/http/server.go#L3329-L3349
// uses this value to resume serving on the connection without explicitly
// registering an error. As mentioned in net/net.go Error interface temporary
// errors are deprecated and may not be supported in the future:
// https://github.com/golang/go/blob/go1.23.7/src/net/net.go#L419, hence we need
// a check that cipherRestrictError always implements the net.Error interface
// fully.
// TODO(souravcrl): update the accept handler to just log an error and continue
// processing new connection requests
func (e *cipherRestrictError) Temporary() bool { return true }

var _ error = (*cipherRestrictError)(nil)

// We implement net.Error the same way that context.DeadlineExceeded does, so
// that people looking for net.Error attributes will still find them.
var _ net.Error = (*cipherRestrictError)(nil)
