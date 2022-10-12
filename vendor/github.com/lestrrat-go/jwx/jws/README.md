# JWS [![Go Reference](https://pkg.go.dev/badge/github.com/lestrrat-go/jwx/jws.svg)](https://pkg.go.dev/github.com/lestrrat-go/jwx/jws)

Package jws implements JWS as described in [RFC7515](https://tools.ietf.org/html/rfc7515) and [RFC7797](https://tools.ietf.org/html/rfc7797)

* Parse and generate compact or JSON serializations
* Sign and verify arbitrary payload
* Use any of the keys supported in [github.com/lestrrat-go/jwx/jwk](../jwk)
* Add arbitrary fields in the JWS object
* Ability to add/replace existing signature methods
* Respect "b64" settings for RFC7797

How-to style documentation can be found in the [docs directory](../docs).

Examples are located in the examples directory ([jws_example_test.go](../examples/jws_example_test.go))

Supported signature algorithms:

| Algorithm                               | Supported? | Constant in [jwa](../jwa) |
|:----------------------------------------|:-----------|:-------------------------|
| HMAC using SHA-256                      | YES        | jwa.HS256                |
| HMAC using SHA-384                      | YES        | jwa.HS384                |
| HMAC using SHA-512                      | YES        | jwa.HS512                |
| RSASSA-PKCS-v1.5 using SHA-256          | YES        | jwa.RS256                |
| RSASSA-PKCS-v1.5 using SHA-384          | YES        | jwa.RS384                |
| RSASSA-PKCS-v1.5 using SHA-512          | YES        | jwa.RS512                |
| ECDSA using P-256 and SHA-256           | YES        | jwa.ES256                |
| ECDSA using P-384 and SHA-384           | YES        | jwa.ES384                |
| ECDSA using P-521 and SHA-512           | YES        | jwa.ES512                |
| ECDSA using secp256k1 and SHA-256 (2)   | YES        | jwa.ES256K               |
| RSASSA-PSS using SHA256 and MGF1-SHA256 | YES        | jwa.PS256                |
| RSASSA-PSS using SHA384 and MGF1-SHA384 | YES        | jwa.PS384                |
| RSASSA-PSS using SHA512 and MGF1-SHA512 | YES        | jwa.PS512                |
| EdDSA (1)                               | YES        | jwa.EdDSA                |

* Note 1: Experimental
* Note 2: Experimental, and must be toggled using `-tags jwx_es256k` build tag

# SYNOPSIS

## Sign and verify arbitrary data

```go
import(
  "crypto/rand"
  "crypto/rsa"
  "log"

  "github.com/lestrrat-go/jwx/jwa"
  "github.com/lestrrat-go/jwx/jws"
)

func main() {
  privkey, err := rsa.GenerateKey(rand.Reader, 2048)
  if err != nil {
    log.Printf("failed to generate private key: %s", err)
    return
  }

  buf, err := jws.Sign([]byte("Lorem ipsum"), jwa.RS256, privkey)
  if err != nil {
    log.Printf("failed to created JWS message: %s", err)
    return
  }

  // When you receive a JWS message, you can verify the signature
  // and grab the payload sent in the message in one go:
  verified, err := jws.Verify(buf, jwa.RS256, &privkey.PublicKey)
  if err != nil {
    log.Printf("failed to verify message: %s", err)
    return
  }

  log.Printf("signed message verified! -> %s", verified)
}
```

## Programatically manipulate `jws.Message`

```go
func ExampleMessage() {
	// initialization for the following variables have been omitted.
	// please see jws_example_test.go for details
	var decodedPayload, decodedSig1, decodedSig2 []byte
	var public1, protected1, public2, protected2 jws.Header

	// Construct a message. DO NOT use values that are base64 encoded
	m := jws.NewMessage().
		SetPayload(decodedPayload).
		AppendSignature(
			jws.NewSignature().
				SetSignature(decodedSig1).
				SetProtectedHeaders(public1).
				SetPublicHeaders(protected1),
		).
		AppendSignature(
			jws.NewSignature().
				SetSignature(decodedSig2).
				SetProtectedHeaders(public2).
				SetPublicHeaders(protected2),
		)

	buf, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}

	_ = buf
}
```

