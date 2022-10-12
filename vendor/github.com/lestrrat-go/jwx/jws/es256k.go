// +build jwx_es256k

package jws

import (
	"github.com/lestrrat-go/jwx/jwa"
)

func init() {
	addAlgorithmForKeyType(jwa.EC, jwa.ES256K)
}
