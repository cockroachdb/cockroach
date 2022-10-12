# JWE [![Go Reference](https://pkg.go.dev/badge/github.com/lestrrat-go/jwx/jwe.svg)](https://pkg.go.dev/github.com/lestrrat-go/jwx/jwe)

Package jwe implements JWE as described in [RFC7516](https://tools.ietf.org/html/rfc7516)

* Encrypt and Decrypt arbitrary data
* Content compression and decompression
* Add arbitrary fields in the JWE header object

How-to style documentation can be found in the [docs directory](../docs).

Examples are located in the examples directory ([jwe_example_test.go](../examples/jwe_example_test.go))

Supported key encryption algorithm:

| Algorithm                                | Supported? | Constant in [jwa](../jwa) |
|:-----------------------------------------|:-----------|:-------------------------|
| RSA-PKCS1v1.5                            | YES        | jwa.RSA1_5               |
| RSA-OAEP-SHA1                            | YES        | jwa.RSA_OAEP             |
| RSA-OAEP-SHA256                          | YES        | jwa.RSA_OAEP_256         |
| AES key wrap (128)                       | YES        | jwa.A128KW               |
| AES key wrap (192)                       | YES        | jwa.A192KW               |
| AES key wrap (256)                       | YES        | jwa.A256KW               |
| Direct encryption                        | YES (1)    | jwa.DIRECT               |
| ECDH-ES                                  | YES (1)    | jwa.ECDH_ES              |
| ECDH-ES + AES key wrap (128)             | YES        | jwa.ECDH_ES_A128KW       |
| ECDH-ES + AES key wrap (192)             | YES        | jwa.ECDH_ES_A192KW       |
| ECDH-ES + AES key wrap (256)             | YES        | jwa.ECDH_ES_A256KW       |
| AES-GCM key wrap (128)                   | YES        | jwa.A128GCMKW            |
| AES-GCM key wrap (192)                   | YES        | jwa.A192GCMKW            |
| AES-GCM key wrap (256)                   | YES        | jwa.A256GCMKW            |
| PBES2 + HMAC-SHA256 + AES key wrap (128) | YES        | jwa.PBES2_HS256_A128KW   |
| PBES2 + HMAC-SHA384 + AES key wrap (192) | YES        | jwa.PBES2_HS384_A192KW   |
| PBES2 + HMAC-SHA512 + AES key wrap (256) | YES        | jwa.PBES2_HS512_A256KW   |

* Note 1: Single-recipient only

Supported content encryption algorithm:

| Algorithm                   | Supported? | Constant in [jwa](../jwa) |
|:----------------------------|:-----------|:--------------------------|
| AES-CBC + HMAC-SHA256 (128) | YES        | jwa.A128CBC_HS256         |
| AES-CBC + HMAC-SHA384 (192) | YES        | jwa.A192CBC_HS384         |
| AES-CBC + HMAC-SHA512 (256) | YES        | jwa.A256CBC_HS512         |
| AES-GCM (128)               | YES        | jwa.A128GCM               |
| AES-GCM (192)               | YES        | jwa.A192GCM               |
| AES-GCM (256)               | YES        | jwa.A256GCM               |

# SYNOPSIS

## Encrypt data

```go
func ExampleEncrypt() {
  privkey, err := rsa.GenerateKey(rand.Reader, 2048)
  if err != nil {
    log.Printf("failed to generate private key: %s", err)
    return
  }

  payload := []byte("Lorem Ipsum")

  encrypted, err := jwe.Encrypt(payload, jwa.RSA1_5, &privkey.PublicKey, jwa.A128CBC_HS256, jwa.NoCompress)
  if err != nil {
    log.Printf("failed to encrypt payload: %s", err)
    return
  }
  _ = encrypted
  // OUTPUT:
}
```

## Decrypt data

```go
func ExampleDecrypt() {
  privkey, encrypted, err := exampleGenPayload()
  if err != nil {
    log.Printf("failed to generate encrypted payload: %s", err)
    return
  }

  decrypted, err := jwe.Decrypt(encrypted, jwa.RSA1_5, privkey)
  if err != nil {
    log.Printf("failed to decrypt: %s", err)
    return
  }

  if string(decrypted) != "Lorem Ipsum" {
    log.Printf("WHAT?!")
    return
  }
  // OUTPUT:
}
```
