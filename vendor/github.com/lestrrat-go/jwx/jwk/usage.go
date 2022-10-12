package jwk

import "github.com/pkg/errors"

func (k KeyUsageType) String() string {
	return string(k)
}

func (k *KeyUsageType) Accept(v interface{}) error {
	switch v := v.(type) {
	case KeyUsageType:
		switch v {
		case ForSignature, ForEncryption:
			*k = v
			return nil
		default:
			return errors.Errorf("invalid key usage type %s", v)
		}
	case string:
		switch v {
		case ForSignature.String(), ForEncryption.String():
			*k = KeyUsageType(v)
			return nil
		default:
			return errors.Errorf("invalid key usage type %s", v)
		}
	}

	return errors.Errorf("invalid value for key usage type %s", v)
}
