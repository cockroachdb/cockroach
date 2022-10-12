package x25519

import (
	"bytes"
	"crypto"
	cryptorand "crypto/rand"
	"io"

	"golang.org/x/crypto/curve25519"

	"github.com/pkg/errors"
)

// This mirrors ed25519's structure for private/public "keys". jwx
// requires dedicated types for these as they drive
// serialization/deserialization logic, as well as encryption types.
//
// Note that with the x25519 scheme, the private key is a sequence of
// 32 bytes, while the public key is the result of X25519(private,
// basepoint).
//
// Portions of this file are from Go's ed25519.go, which is
// Copyright 2016 The Go Authors. All rights reserved.

const (
	// PublicKeySize is the size, in bytes, of public keys as used in this package.
	PublicKeySize = 32
	// PrivateKeySize is the size, in bytes, of private keys as used in this package.
	PrivateKeySize = 64
	// SeedSize is the size, in bytes, of private key seeds. These are the private key representations used by RFC 8032.
	SeedSize = 32
)

// PublicKey is the type of X25519 public keys
type PublicKey []byte

// Any methods implemented on PublicKey might need to also be implemented on
// PrivateKey, as the latter embeds the former and will expose its methods.

// Equal reports whether pub and x have the same value.
func (pub PublicKey) Equal(x crypto.PublicKey) bool {
	xx, ok := x.(PublicKey)
	if !ok {
		return false
	}
	return bytes.Equal(pub, xx)
}

// PrivateKey is the type of X25519 private key
type PrivateKey []byte

// Public returns the PublicKey corresponding to priv.
func (priv PrivateKey) Public() crypto.PublicKey {
	publicKey := make([]byte, PublicKeySize)
	copy(publicKey, priv[SeedSize:])
	return PublicKey(publicKey)
}

// Equal reports whether priv and x have the same value.
func (priv PrivateKey) Equal(x crypto.PrivateKey) bool {
	xx, ok := x.(PrivateKey)
	if !ok {
		return false
	}
	return bytes.Equal(priv, xx)
}

// Seed returns the private key seed corresponding to priv. It is provided for
// interoperability with RFC 7748. RFC 7748's private keys correspond to seeds
// in this package.
func (priv PrivateKey) Seed() []byte {
	seed := make([]byte, SeedSize)
	copy(seed, priv[:SeedSize])
	return seed
}

// NewKeyFromSeed calculates a private key from a seed. It will return
// an error if len(seed) is not SeedSize. This function is provided
// for interoperability with RFC 7748. RFC 7748's private keys
// correspond to seeds in this package.
func NewKeyFromSeed(seed []byte) (PrivateKey, error) {
	privateKey := make([]byte, PrivateKeySize)
	if len(seed) != SeedSize {
		return nil, errors.Errorf("unexpected seed size: %d", len(seed))
	}
	copy(privateKey, seed)
	public, err := curve25519.X25519(seed, curve25519.Basepoint)
	if err != nil {
		return nil, errors.Wrap(err, "failed to compute public key")
	}
	copy(privateKey[SeedSize:], public)

	return privateKey, nil
}

// GenerateKey generates a public/private key pair using entropy from rand.
// If rand is nil, crypto/rand.Reader will be used.
func GenerateKey(rand io.Reader) (PublicKey, PrivateKey, error) {
	if rand == nil {
		rand = cryptorand.Reader
	}

	seed := make([]byte, SeedSize)
	if _, err := io.ReadFull(rand, seed); err != nil {
		return nil, nil, err
	}

	privateKey, err := NewKeyFromSeed(seed)
	if err != nil {
		return nil, nil, err
	}
	publicKey := make([]byte, PublicKeySize)
	copy(publicKey, privateKey[SeedSize:])

	return publicKey, privateKey, nil
}
