// +build jwx_es256k

package jwk

import (
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/lestrrat-go/jwx/internal/ecutil"
	"github.com/lestrrat-go/jwx/jwa"
)

func init() {
	ecutil.RegisterCurve(secp256k1.S256(), jwa.Secp256k1)
}
