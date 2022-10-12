// +build jwx_es256k

package jwa

// This constant is only available if compiled with jwx_es256k build tag
const Secp256k1 EllipticCurveAlgorithm = "secp256k1"

func init() {
	allEllipticCurveAlgorithms[Secp256k1] = struct{}{}
}
