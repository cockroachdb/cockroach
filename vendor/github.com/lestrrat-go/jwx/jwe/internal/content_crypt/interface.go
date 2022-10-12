package content_crypt //nolint:golint

import (
	"github.com/lestrrat-go/jwx/jwa"
	"github.com/lestrrat-go/jwx/jwe/internal/cipher"
)

// Generic encrypts a message by applying all the necessary
// modifications to the keys and the contents
type Generic struct {
	alg     jwa.ContentEncryptionAlgorithm
	keysize int
	tagsize int
	cipher  cipher.ContentCipher
}

type Cipher interface {
	Decrypt([]byte, []byte, []byte, []byte, []byte) ([]byte, error)
	KeySize() int
}
