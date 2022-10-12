# JWT [![Go Reference](https://pkg.go.dev/badge/github.com/lestrrat-go/jwx/jwt.svg)](https://pkg.go.dev/github.com/lestrrat-go/jwx/jwt)

Package jwt implements JSON Web Tokens as described in [RFC7519](https://tools.ietf.org/html/rfc7519).

* Convenience methods for oft-used keys ("aud", "sub", "iss", etc)
* Convenience functions to extract/parse from http.Request, http.Header, url.Values
* Ability to Get/Set arbitrary keys
* Conversion to and from JSON
* Generate signed tokens
* Verify signed tokens
* Extra support for OpenID tokens via [github.com/lestrrat-go/jwx/jwt/openid](./jwt/openid)

How-to style documentation can be found in the [docs directory](../docs).

More examples are located in the examples directory ([jwt_example_test.go](../examples/jwt_example_test.go))

# SYNOPSIS

## Verify a signed JWT

```go
  token, err := jwt.Parse(payload, jwt.WithKeySet(keyset))
  if err != nil {
    fmt.Printf("failed to parse payload: %s\n", err)
  }
```

## Token Usage

```go
func ExampleJWT() {
  const aLongLongTimeAgo = 233431200

  t := jwt.New()
  t.Set(jwt.SubjectKey, `https://github.com/lestrrat-go/jwx/jwt`)
  t.Set(jwt.AudienceKey, `Golang Users`)
  t.Set(jwt.IssuedAtKey, time.Unix(aLongLongTimeAgo, 0))
  t.Set(`privateClaimKey`, `Hello, World!`)

  buf, err := json.MarshalIndent(t, "", "  ")
  if err != nil {
    fmt.Printf("failed to generate JSON: %s\n", err)
    return
  }

  fmt.Printf("%s\n", buf)
  fmt.Printf("aud -> '%s'\n", t.Audience())
  fmt.Printf("iat -> '%s'\n", t.IssuedAt().Format(time.RFC3339))
  if v, ok := t.Get(`privateClaimKey`); ok {
    fmt.Printf("privateClaimKey -> '%s'\n", v)
  }
  fmt.Printf("sub -> '%s'\n", t.Subject())

  key, err := rsa.GenerateKey(rand.Reader, 2048)
  if err != nil {
    log.Printf("failed to generate private key: %s", err)
    return
  }

  {
    // Signing a token (using raw rsa.PrivateKey)
    signed, err := jwt.Sign(t, jwa.RS256, key)
    if err != nil {
      log.Printf("failed to sign token: %s", err)
      return
    }
    _ = signed
  }

  {
    // Signing a token (using JWK)
    jwkKey, err := jwk.New(key)
    if err != nil {
      log.Printf("failed to create JWK key: %s", err)
      return
    }

    signed, err := jwt.Sign(t, jwa.RS256, jwkKey)
    if err != nil {
      log.Printf("failed to sign token: %s", err)
      return
    }
    _ = signed
  }
}
```

## OpenID Claims

`jwt` package can work with token types other than the default one.
For OpenID claims, use the token created by `openid.New()`, or
use the `jwt.WithToken(openid.New())`. If you need to use other specialized
claims, use `jwt.WithToken()` to specify the exact token type

```go
func Example_openid() {
  const aLongLongTimeAgo = 233431200

  t := openid.New()
  t.Set(jwt.SubjectKey, `https://github.com/lestrrat-go/jwx/jwt`)
  t.Set(jwt.AudienceKey, `Golang Users`)
  t.Set(jwt.IssuedAtKey, time.Unix(aLongLongTimeAgo, 0))
  t.Set(`privateClaimKey`, `Hello, World!`)

  addr := openid.NewAddress()
  addr.Set(openid.AddressPostalCodeKey, `105-0011`)
  addr.Set(openid.AddressCountryKey, `日本`)
  addr.Set(openid.AddressRegionKey, `東京都`)
  addr.Set(openid.AddressLocalityKey, `港区`)
  addr.Set(openid.AddressStreetAddressKey, `芝公園 4-2-8`)
  t.Set(openid.AddressKey, addr)

  buf, err := json.MarshalIndent(t, "", "  ")
  if err != nil {
    fmt.Printf("failed to generate JSON: %s\n", err)
    return
  }
  fmt.Printf("%s\n", buf)

  t2, err := jwt.Parse(buf, jwt.WithToken(openid.New()))
  if err != nil {
    fmt.Printf("failed to parse JSON: %s\n", err)
    return
  }
  if _, ok := t2.(openid.Token); !ok {
    fmt.Printf("using jwt.WithToken(openid.New()) creates an openid.Token instance")
    return
  }
}
```

# FAQ

## Why is `jwt.Token` an interface?

In this package, `jwt.Token` is an interface. This is not an arbitrary choice: there are actual reason for the type being an interface.

We understand that if you are migrating from another library this may be a deal breaker, but we hope you can at least appreciate the fact that this was not done arbitrarily, and that there were real technical trade offs that were evaluated.

### No uninitialized tokens

First and foremost, by making it an interface, you cannot use an uninitialized token:

```go
var token1 jwt.Token // this is nil, you can't just start using this
if err := json.Unmarshal(data, &token1); err != nil { // so you can't do this
   ...
}

// But you _can_ do this, and we _want_ you to do this so the object is properly initialized
token2 = jwt.New()
if err := json.Unmarshal(data, &token2); err != nil { // actually, in practice you should use jwt.Parse()
   ....
}
```

### But why does it need to be initialized?

There are several reasons, but one of the reasons is that I'm using a sync.Mutex to avoid races. We want this to be properly initialized.

The other reason is that we support custom claims out of the box. The `map[string]interface{}` container is initialized during new. This is important when checking for equality using reflect-y methods (akin to `reflect.DeepEqual`), because if you allowed zero values, you could end up with "empty" tokens, that actually differ. Consider the following:

```go
// assume jwt.Token was s struct, not an interface
token1 := jwt.Token{ privateClaims: make(map[string]interface{}) }
token2 := jwt.Token{ privateClaims: nil }
```

These are semantically equivalent, but users would need to be aware of this difference when comparing values. By forcing the user to use a constructor, we can force a uniform empty state.

### Standard way to store values

Unlike some other libraries, this library allows you to store standard claims and non-standard claims in the same token.

You _want_ to store standard claims in a properly typed field, which we do for fields like "iss", "nbf", etc.
But for non-standard claims, there is just no way of doing this, so we _have_ to use a container like `map[string]interface{}`

This means that if you allow direct access to these fields via a struct, you will have two different ways to access the claims, which is confusing:

```go
tok.Issuer = ...
tok.PrivateClaims["foo"] = ...
```

So we want to hide where this data is stored, and use a standard method like `Set()` and `Get()` to store all the values.
At this point you are effectively going to hide the implementation detail from the user, so you end up with a struct like below, which is fundamentally not so different from providing just an interface{}:

```go
type Token struct {
  // unexported fields
}

func (tok *Token) Set(...) { ... }
```

### Use of pointers to store values

We wanted to differentiate the state between a claim being uninitialized, and a claim being initialized to empty.

So we use pointers to store values:

```go
type stdToken struct {
  ....
  issuer *string // if nil, uninitialized. if &(""), initialized to empty
}
```

This is fine for us, but we doubt that this would be something users would want to do.
This is a subtle difference, but cluttering up the API with slight variations of the same type (i.e. pointers vs non-pointers) seemed like a bad idea to us.

```go
token.Issuer = &issuer // want to avoid this

token.Set(jwt.IssuerKey, "foobar") // so this is what we picked
```

This way users no longer need to care how the data is internally stored.

### Allow more than one type of token through the same interface

`dgrijalva/jwt-go` does this in a different way, but we felt that it would be more intuitive for all tokens to follow a single interface so there is fewer type conversions required.

See the `openid` token for an example.
