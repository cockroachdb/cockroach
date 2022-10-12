package keyenc

import (
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"hash"
	"io"

	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/pbkdf2"

	"github.com/lestrrat-go/jwx/internal/ecutil"
	"github.com/lestrrat-go/jwx/jwa"
	contentcipher "github.com/lestrrat-go/jwx/jwe/internal/cipher"
	"github.com/lestrrat-go/jwx/jwe/internal/concatkdf"
	"github.com/lestrrat-go/jwx/jwe/internal/keygen"
	"github.com/lestrrat-go/jwx/x25519"
	"github.com/pkg/errors"
)

func NewNoop(alg jwa.KeyEncryptionAlgorithm, sharedkey []byte) (*Noop, error) {
	return &Noop{
		alg:       alg,
		sharedkey: sharedkey,
	}, nil
}

func (kw *Noop) Algorithm() jwa.KeyEncryptionAlgorithm {
	return kw.alg
}

func (kw *Noop) SetKeyID(v string) {
	kw.keyID = v
}

func (kw *Noop) KeyID() string {
	return kw.keyID
}

func (kw *Noop) Encrypt(cek []byte) (keygen.ByteSource, error) {
	return keygen.ByteKey(kw.sharedkey), nil
}

// NewAES creates a key-wrap encrypter using AES.
// Although the name suggests otherwise, this does the decryption as well.
func NewAES(alg jwa.KeyEncryptionAlgorithm, sharedkey []byte) (*AES, error) {
	return &AES{
		alg:       alg,
		sharedkey: sharedkey,
	}, nil
}

// Algorithm returns the key encryption algorithm being used
func (kw *AES) Algorithm() jwa.KeyEncryptionAlgorithm {
	return kw.alg
}

func (kw *AES) SetKeyID(v string) {
	kw.keyID = v
}

// KeyID returns the key ID associated with this encrypter
func (kw *AES) KeyID() string {
	return kw.keyID
}

// Decrypt decrypts the encrypted key using AES key unwrap
func (kw *AES) Decrypt(enckey []byte) ([]byte, error) {
	block, err := aes.NewCipher(kw.sharedkey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create cipher from shared key")
	}

	cek, err := Unwrap(block, enckey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unwrap data")
	}
	return cek, nil
}

// KeyEncrypt encrypts the given content encryption key
func (kw *AES) Encrypt(cek []byte) (keygen.ByteSource, error) {
	block, err := aes.NewCipher(kw.sharedkey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create cipher from shared key")
	}
	encrypted, err := Wrap(block, cek)
	if err != nil {
		return nil, errors.Wrap(err, `keywrap: failed to wrap key`)
	}
	return keygen.ByteKey(encrypted), nil
}

func NewAESGCMEncrypt(alg jwa.KeyEncryptionAlgorithm, sharedkey []byte) (*AESGCMEncrypt, error) {
	return &AESGCMEncrypt{
		algorithm: alg,
		sharedkey: sharedkey,
	}, nil
}

func (kw AESGCMEncrypt) Algorithm() jwa.KeyEncryptionAlgorithm {
	return kw.algorithm
}

func (kw *AESGCMEncrypt) SetKeyID(v string) {
	kw.keyID = v
}

func (kw AESGCMEncrypt) KeyID() string {
	return kw.keyID
}

func (kw AESGCMEncrypt) Encrypt(cek []byte) (keygen.ByteSource, error) {
	block, err := aes.NewCipher(kw.sharedkey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create cipher from shared key")
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create gcm from cipher")
	}

	iv := make([]byte, aesgcm.NonceSize())
	_, err = io.ReadFull(rand.Reader, iv)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get random iv")
	}

	encrypted := aesgcm.Seal(nil, iv, cek, nil)
	tag := encrypted[len(encrypted)-aesgcm.Overhead():]
	ciphertext := encrypted[:len(encrypted)-aesgcm.Overhead()]
	return keygen.ByteWithIVAndTag{
		ByteKey: ciphertext,
		IV:      iv,
		Tag:     tag,
	}, nil
}

func NewPBES2Encrypt(alg jwa.KeyEncryptionAlgorithm, password []byte) (*PBES2Encrypt, error) {
	var hashFunc func() hash.Hash
	var keylen int
	switch alg {
	case jwa.PBES2_HS256_A128KW:
		hashFunc = sha256.New
		keylen = 16
	case jwa.PBES2_HS384_A192KW:
		hashFunc = sha512.New384
		keylen = 24
	case jwa.PBES2_HS512_A256KW:
		hashFunc = sha512.New
		keylen = 32
	default:
		return nil, errors.Errorf("unexpected key encryption algorithm %s", alg)
	}
	return &PBES2Encrypt{
		algorithm: alg,
		password:  password,
		hashFunc:  hashFunc,
		keylen:    keylen,
	}, nil
}

func (kw PBES2Encrypt) Algorithm() jwa.KeyEncryptionAlgorithm {
	return kw.algorithm
}

func (kw *PBES2Encrypt) SetKeyID(v string) {
	kw.keyID = v
}

func (kw PBES2Encrypt) KeyID() string {
	return kw.keyID
}

func (kw PBES2Encrypt) Encrypt(cek []byte) (keygen.ByteSource, error) {
	count := 10000
	salt := make([]byte, kw.keylen)
	_, err := io.ReadFull(rand.Reader, salt)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get random salt")
	}

	fullsalt := []byte(kw.algorithm)
	fullsalt = append(fullsalt, byte(0))
	fullsalt = append(fullsalt, salt...)
	sharedkey := pbkdf2.Key(kw.password, fullsalt, count, kw.keylen, kw.hashFunc)

	block, err := aes.NewCipher(sharedkey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create cipher from shared key")
	}
	encrypted, err := Wrap(block, cek)
	if err != nil {
		return nil, errors.Wrap(err, `keywrap: failed to wrap key`)
	}
	return keygen.ByteWithSaltAndCount{
		ByteKey: encrypted,
		Salt:    salt,
		Count:   count,
	}, nil
}

// NewECDHESEncrypt creates a new key encrypter based on ECDH-ES
func NewECDHESEncrypt(alg jwa.KeyEncryptionAlgorithm, enc jwa.ContentEncryptionAlgorithm, keysize int, keyif interface{}) (*ECDHESEncrypt, error) {
	var generator keygen.Generator
	var err error
	switch key := keyif.(type) {
	case *ecdsa.PublicKey:
		generator, err = keygen.NewEcdhes(alg, enc, keysize, key)
	case x25519.PublicKey:
		generator, err = keygen.NewX25519(alg, enc, keysize, key)
	default:
		return nil, errors.Errorf("unexpected key type %T", keyif)
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to create key generator")
	}
	return &ECDHESEncrypt{
		algorithm: alg,
		generator: generator,
	}, nil
}

// Algorithm returns the key encryption algorithm being used
func (kw ECDHESEncrypt) Algorithm() jwa.KeyEncryptionAlgorithm {
	return kw.algorithm
}

func (kw *ECDHESEncrypt) SetKeyID(v string) {
	kw.keyID = v
}

// KeyID returns the key ID associated with this encrypter
func (kw ECDHESEncrypt) KeyID() string {
	return kw.keyID
}

// KeyEncrypt encrypts the content encryption key using ECDH-ES
func (kw ECDHESEncrypt) Encrypt(cek []byte) (keygen.ByteSource, error) {
	kg, err := kw.generator.Generate()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create key generator")
	}

	bwpk, ok := kg.(keygen.ByteWithECPublicKey)
	if !ok {
		return nil, errors.New("key generator generated invalid key (expected ByteWithECPrivateKey)")
	}

	if kw.algorithm == jwa.ECDH_ES {
		return bwpk, nil
	}

	block, err := aes.NewCipher(bwpk.Bytes())
	if err != nil {
		return nil, errors.Wrap(err, "failed to generate cipher from generated key")
	}

	jek, err := Wrap(block, cek)
	if err != nil {
		return nil, errors.Wrap(err, "failed to wrap data")
	}

	bwpk.ByteKey = keygen.ByteKey(jek)

	return bwpk, nil
}

// NewECDHESDecrypt creates a new key decrypter using ECDH-ES
func NewECDHESDecrypt(keyalg jwa.KeyEncryptionAlgorithm, contentalg jwa.ContentEncryptionAlgorithm, pubkey interface{}, apu, apv []byte, privkey interface{}) *ECDHESDecrypt {
	return &ECDHESDecrypt{
		keyalg:     keyalg,
		contentalg: contentalg,
		apu:        apu,
		apv:        apv,
		privkey:    privkey,
		pubkey:     pubkey,
	}
}

// Algorithm returns the key encryption algorithm being used
func (kw ECDHESDecrypt) Algorithm() jwa.KeyEncryptionAlgorithm {
	return kw.keyalg
}

func DeriveZ(privkeyif interface{}, pubkeyif interface{}) ([]byte, error) {
	switch privkeyif.(type) {
	case x25519.PrivateKey:
		privkey, ok := privkeyif.(x25519.PrivateKey)
		if !ok {
			return nil, errors.Errorf(`private key must be x25519.PrivateKey, was: %T`, privkeyif)
		}
		pubkey, ok := pubkeyif.(x25519.PublicKey)
		if !ok {
			return nil, errors.Errorf(`public key must be x25519.PublicKey, was: %T`, pubkeyif)
		}
		return curve25519.X25519(privkey.Seed(), pubkey)
	default:
		privkey, ok := privkeyif.(*ecdsa.PrivateKey)
		if !ok {
			return nil, errors.Errorf(`private key must be *ecdsa.PrivateKey, was: %T`, privkeyif)
		}
		pubkey, ok := pubkeyif.(*ecdsa.PublicKey)
		if !ok {
			return nil, errors.Errorf(`public key must be *ecdsa.PublicKey, was: %T`, pubkeyif)
		}
		if !privkey.PublicKey.Curve.IsOnCurve(pubkey.X, pubkey.Y) {
			return nil, errors.New(`public key must be on the same curve as private key`)
		}

		z, _ := privkey.PublicKey.Curve.ScalarMult(pubkey.X, pubkey.Y, privkey.D.Bytes())
		zBytes := ecutil.AllocECPointBuffer(z, privkey.Curve)
		defer ecutil.ReleaseECPointBuffer(zBytes)
		zCopy := make([]byte, len(zBytes))
		copy(zCopy, zBytes)
		return zCopy, nil
	}
}

func DeriveECDHES(alg, apu, apv []byte, privkey interface{}, pubkey interface{}, keysize uint32) ([]byte, error) {
	pubinfo := make([]byte, 4)
	binary.BigEndian.PutUint32(pubinfo, keysize*8)
	zBytes, err := DeriveZ(privkey, pubkey)
	if err != nil {
		return nil, errors.Wrap(err, "unable to determine Z")
	}
	kdf := concatkdf.New(crypto.SHA256, alg, zBytes, apu, apv, pubinfo, []byte{})
	key := make([]byte, keysize)
	if _, err := kdf.Read(key); err != nil {
		return nil, errors.Wrap(err, "failed to read kdf")
	}

	return key, nil
}

// Decrypt decrypts the encrypted key using ECDH-ES
func (kw ECDHESDecrypt) Decrypt(enckey []byte) ([]byte, error) {
	var algBytes []byte
	var keysize uint32

	// Use keyalg except for when jwa.ECDH_ES
	algBytes = []byte(kw.keyalg.String())

	switch kw.keyalg {
	case jwa.ECDH_ES:
		// Create a content cipher from the content encryption algorithm
		c, err := contentcipher.NewAES(kw.contentalg)
		if err != nil {
			return nil, errors.Wrapf(err, `failed to create content cipher for %s`, kw.contentalg)
		}
		keysize = uint32(c.KeySize())
		algBytes = []byte(kw.contentalg.String())
	case jwa.ECDH_ES_A128KW:
		keysize = 16
	case jwa.ECDH_ES_A192KW:
		keysize = 24
	case jwa.ECDH_ES_A256KW:
		keysize = 32
	default:
		return nil, errors.Errorf("invalid ECDH-ES key wrap algorithm (%s)", kw.keyalg)
	}

	key, err := DeriveECDHES(algBytes, kw.apu, kw.apv, kw.privkey, kw.pubkey, keysize)
	if err != nil {
		return nil, errors.Wrap(err, `failed to derive ECDHES encryption key`)
	}

	// ECDH-ES does not wrap keys
	if kw.keyalg == jwa.ECDH_ES {
		return key, nil
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create cipher for ECDH-ES key wrap")
	}

	return Unwrap(block, enckey)
}

// NewRSAOAEPEncrypt creates a new key encrypter using RSA OAEP
func NewRSAOAEPEncrypt(alg jwa.KeyEncryptionAlgorithm, pubkey *rsa.PublicKey) (*RSAOAEPEncrypt, error) {
	switch alg {
	case jwa.RSA_OAEP, jwa.RSA_OAEP_256:
	default:
		return nil, errors.Errorf("invalid RSA OAEP encrypt algorithm (%s)", alg)
	}
	return &RSAOAEPEncrypt{
		alg:    alg,
		pubkey: pubkey,
	}, nil
}

// NewRSAPKCSEncrypt creates a new key encrypter using PKCS1v15
func NewRSAPKCSEncrypt(alg jwa.KeyEncryptionAlgorithm, pubkey *rsa.PublicKey) (*RSAPKCSEncrypt, error) {
	switch alg {
	case jwa.RSA1_5:
	default:
		return nil, errors.Errorf("invalid RSA PKCS encrypt algorithm (%s)", alg)
	}

	return &RSAPKCSEncrypt{
		alg:    alg,
		pubkey: pubkey,
	}, nil
}

// Algorithm returns the key encryption algorithm being used
func (e RSAPKCSEncrypt) Algorithm() jwa.KeyEncryptionAlgorithm {
	return e.alg
}

func (e *RSAPKCSEncrypt) SetKeyID(v string) {
	e.keyID = v
}

// KeyID returns the key ID associated with this encrypter
func (e RSAPKCSEncrypt) KeyID() string {
	return e.keyID
}

// Algorithm returns the key encryption algorithm being used
func (e RSAOAEPEncrypt) Algorithm() jwa.KeyEncryptionAlgorithm {
	return e.alg
}

func (e *RSAOAEPEncrypt) SetKeyID(v string) {
	e.keyID = v
}

// KeyID returns the key ID associated with this encrypter
func (e RSAOAEPEncrypt) KeyID() string {
	return e.keyID
}

// KeyEncrypt encrypts the content encryption key using RSA PKCS1v15
func (e RSAPKCSEncrypt) Encrypt(cek []byte) (keygen.ByteSource, error) {
	if e.alg != jwa.RSA1_5 {
		return nil, errors.Errorf("invalid RSA PKCS encrypt algorithm (%s)", e.alg)
	}
	encrypted, err := rsa.EncryptPKCS1v15(rand.Reader, e.pubkey, cek)
	if err != nil {
		return nil, errors.Wrap(err, "failed to encrypt using PKCS1v15")
	}
	return keygen.ByteKey(encrypted), nil
}

// KeyEncrypt encrypts the content encryption key using RSA OAEP
func (e RSAOAEPEncrypt) Encrypt(cek []byte) (keygen.ByteSource, error) {
	var hash hash.Hash
	switch e.alg {
	case jwa.RSA_OAEP:
		hash = sha1.New()
	case jwa.RSA_OAEP_256:
		hash = sha256.New()
	default:
		return nil, errors.New("failed to generate key encrypter for RSA-OAEP: RSA_OAEP/RSA_OAEP_256 required")
	}
	encrypted, err := rsa.EncryptOAEP(hash, rand.Reader, e.pubkey, cek, []byte{})
	if err != nil {
		return nil, errors.Wrap(err, `failed to OAEP encrypt`)
	}
	return keygen.ByteKey(encrypted), nil
}

// NewRSAPKCS15Decrypt creates a new decrypter using RSA PKCS1v15
func NewRSAPKCS15Decrypt(alg jwa.KeyEncryptionAlgorithm, privkey *rsa.PrivateKey, keysize int) *RSAPKCS15Decrypt {
	generator := keygen.NewRandom(keysize * 2)
	return &RSAPKCS15Decrypt{
		alg:       alg,
		privkey:   privkey,
		generator: generator,
	}
}

// Algorithm returns the key encryption algorithm being used
func (d RSAPKCS15Decrypt) Algorithm() jwa.KeyEncryptionAlgorithm {
	return d.alg
}

// Decrypt decrypts the encrypted key using RSA PKCS1v1.5
func (d RSAPKCS15Decrypt) Decrypt(enckey []byte) ([]byte, error) {
	// Hey, these notes and workarounds were stolen from go-jose
	defer func() {
		// DecryptPKCS1v15SessionKey sometimes panics on an invalid payload
		// because of an index out of bounds error, which we want to ignore.
		// This has been fixed in Go 1.3.1 (released 2014/08/13), the recover()
		// only exists for preventing crashes with unpatched versions.
		// See: https://groups.google.com/forum/#!topic/golang-dev/7ihX6Y6kx9k
		// See: https://code.google.com/p/go/source/detail?r=58ee390ff31602edb66af41ed10901ec95904d33
		_ = recover()
	}()

	// Perform some input validation.
	expectedlen := d.privkey.PublicKey.N.BitLen() / 8
	if expectedlen != len(enckey) {
		// Input size is incorrect, the encrypted payload should always match
		// the size of the public modulus (e.g. using a 2048 bit key will
		// produce 256 bytes of output). Reject this since it's invalid input.
		return nil, fmt.Errorf(
			"input size for key decrypt is incorrect (expected %d, got %d)",
			expectedlen,
			len(enckey),
		)
	}

	var err error

	bk, err := d.generator.Generate()
	if err != nil {
		return nil, errors.New("failed to generate key")
	}
	cek := bk.Bytes()

	// When decrypting an RSA-PKCS1v1.5 payload, we must take precautions to
	// prevent chosen-ciphertext attacks as described in RFC 3218, "Preventing
	// the Million Message Attack on Cryptographic Message Syntax". We are
	// therefore deliberately ignoring errors here.
	err = rsa.DecryptPKCS1v15SessionKey(rand.Reader, d.privkey, enckey, cek)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decrypt via PKCS1v15")
	}

	return cek, nil
}

// NewRSAOAEPDecrypt creates a new key decrypter using RSA OAEP
func NewRSAOAEPDecrypt(alg jwa.KeyEncryptionAlgorithm, privkey *rsa.PrivateKey) (*RSAOAEPDecrypt, error) {
	switch alg {
	case jwa.RSA_OAEP, jwa.RSA_OAEP_256:
	default:
		return nil, errors.Errorf("invalid RSA OAEP decrypt algorithm (%s)", alg)
	}

	return &RSAOAEPDecrypt{
		alg:     alg,
		privkey: privkey,
	}, nil
}

// Algorithm returns the key encryption algorithm being used
func (d RSAOAEPDecrypt) Algorithm() jwa.KeyEncryptionAlgorithm {
	return d.alg
}

// Decrypt decrypts the encrypted key using RSA OAEP
func (d RSAOAEPDecrypt) Decrypt(enckey []byte) ([]byte, error) {
	var hash hash.Hash
	switch d.alg {
	case jwa.RSA_OAEP:
		hash = sha1.New()
	case jwa.RSA_OAEP_256:
		hash = sha256.New()
	default:
		return nil, errors.New("failed to generate key encrypter for RSA-OAEP: RSA_OAEP/RSA_OAEP_256 required")
	}
	return rsa.DecryptOAEP(hash, rand.Reader, d.privkey, enckey, []byte{})
}

// Decrypt for DirectDecrypt does not do anything other than
// return a copy of the embedded key
func (d DirectDecrypt) Decrypt() ([]byte, error) {
	cek := make([]byte, len(d.Key))
	copy(cek, d.Key)
	return cek, nil
}

var keywrapDefaultIV = []byte{0xa6, 0xa6, 0xa6, 0xa6, 0xa6, 0xa6, 0xa6, 0xa6}

const keywrapChunkLen = 8

func Wrap(kek cipher.Block, cek []byte) ([]byte, error) {
	if len(cek)%8 != 0 {
		return nil, errors.New(`keywrap input must be 8 byte blocks`)
	}

	n := len(cek) / keywrapChunkLen
	r := make([][]byte, n)

	for i := 0; i < n; i++ {
		r[i] = make([]byte, keywrapChunkLen)
		copy(r[i], cek[i*keywrapChunkLen:])
	}

	buffer := make([]byte, keywrapChunkLen*2)
	tBytes := make([]byte, keywrapChunkLen)
	copy(buffer, keywrapDefaultIV)

	for t := 0; t < 6*n; t++ {
		copy(buffer[keywrapChunkLen:], r[t%n])

		kek.Encrypt(buffer, buffer)

		binary.BigEndian.PutUint64(tBytes, uint64(t+1))

		for i := 0; i < keywrapChunkLen; i++ {
			buffer[i] = buffer[i] ^ tBytes[i]
		}
		copy(r[t%n], buffer[keywrapChunkLen:])
	}

	out := make([]byte, (n+1)*keywrapChunkLen)
	copy(out, buffer[:keywrapChunkLen])
	for i := range r {
		copy(out[(i+1)*8:], r[i])
	}

	return out, nil
}

func Unwrap(block cipher.Block, ciphertxt []byte) ([]byte, error) {
	if len(ciphertxt)%keywrapChunkLen != 0 {
		return nil, errors.Errorf(`keyunwrap input must be %d byte blocks`, keywrapChunkLen)
	}

	n := (len(ciphertxt) / keywrapChunkLen) - 1
	r := make([][]byte, n)

	for i := range r {
		r[i] = make([]byte, keywrapChunkLen)
		copy(r[i], ciphertxt[(i+1)*keywrapChunkLen:])
	}

	buffer := make([]byte, keywrapChunkLen*2)
	tBytes := make([]byte, keywrapChunkLen)
	copy(buffer[:keywrapChunkLen], ciphertxt[:keywrapChunkLen])

	for t := 6*n - 1; t >= 0; t-- {
		binary.BigEndian.PutUint64(tBytes, uint64(t+1))

		for i := 0; i < keywrapChunkLen; i++ {
			buffer[i] = buffer[i] ^ tBytes[i]
		}
		copy(buffer[keywrapChunkLen:], r[t%n])

		block.Decrypt(buffer, buffer)

		copy(r[t%n], buffer[keywrapChunkLen:])
	}

	if subtle.ConstantTimeCompare(buffer[:keywrapChunkLen], keywrapDefaultIV) == 0 {
		return nil, errors.New("key unwrap: failed to unwrap key")
	}

	out := make([]byte, n*keywrapChunkLen)
	for i := range r {
		copy(out[i*keywrapChunkLen:], r[i])
	}

	return out, nil
}
