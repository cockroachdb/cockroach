package sarama

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"hash"
)

// ScramFormatter implementation
// @see: https://github.com/apache/kafka/blob/99b9b3e84f4e98c3f07714e1de6a139a004cbc5b/clients/src/main/java/org/apache/kafka/common/security/scram/internals/ScramFormatter.java#L93
type scramFormatter struct {
	mechanism ScramMechanismType
}

func (s scramFormatter) mac(key []byte) (hash.Hash, error) {
	var m hash.Hash

	switch s.mechanism {
	case SCRAM_MECHANISM_SHA_256:
		m = hmac.New(sha256.New, key)

	case SCRAM_MECHANISM_SHA_512:
		m = hmac.New(sha512.New, key)
	default:
		return nil, ErrUnknownScramMechanism
	}

	return m, nil
}

func (s scramFormatter) hmac(key []byte, extra []byte) ([]byte, error) {
	mac, err := s.mac(key)
	if err != nil {
		return nil, err
	}

	if _, err := mac.Write(extra); err != nil {
		return nil, err
	}
	return mac.Sum(nil), nil
}

func (s scramFormatter) xor(result []byte, second []byte) {
	for i := 0; i < len(result); i++ {
		result[i] = result[i] ^ second[i]
	}
}

func (s scramFormatter) saltedPassword(password []byte, salt []byte, iterations int) ([]byte, error) {
	mac, err := s.mac(password)
	if err != nil {
		return nil, err
	}

	if _, err := mac.Write(salt); err != nil {
		return nil, err
	}
	if _, err := mac.Write([]byte{0, 0, 0, 1}); err != nil {
		return nil, err
	}

	u1 := mac.Sum(nil)
	prev := u1
	result := u1

	for i := 2; i <= iterations; i++ {
		ui, err := s.hmac(password, prev)
		if err != nil {
			return nil, err
		}

		s.xor(result, ui)
		prev = ui
	}

	return result, nil
}
