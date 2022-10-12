package jws

import (
	"net/http"

	"github.com/lestrrat-go/backoff/v2"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/lestrrat-go/option"
)

type Option = option.Interface

type identPayloadSigner struct{}
type identDetachedPayload struct{}
type identHeaders struct{}
type identMessage struct{}
type identFetchBackoff struct{}
type identFetchWhitelist struct{}
type identHTTPClient struct{}
type identJWKSetFetcher struct{}

func WithSigner(signer Signer, key interface{}, public, protected Headers) Option {
	return option.New(identPayloadSigner{}, &payloadSigner{
		signer:    signer,
		key:       key,
		protected: protected,
		public:    public,
	})
}

type SignOption interface {
	Option
	signOption()
}

type signOption struct {
	Option
}

func (*signOption) signOption() {}

// WithHeaders allows you to specify extra header values to include in the
// final JWS message
func WithHeaders(h Headers) SignOption {
	return &signOption{option.New(identHeaders{}, h)}
}

// VerifyOption describes an option that can be passed to the jws.Verify function
type VerifyOption interface {
	Option
	verifyOption()
}

type verifyOption struct {
	Option
}

func (*verifyOption) verifyOption() {}

// WithMessage can be passed to Verify() to obtain the jws.Message upon
// a successful verification.
func WithMessage(m *Message) VerifyOption {
	return &verifyOption{option.New(identMessage{}, m)}
}

type SignVerifyOption interface {
	SignOption
	VerifyOption
}

type signVerifyOption struct {
	Option
}

func (*signVerifyOption) signOption()   {}
func (*signVerifyOption) verifyOption() {}

// WithDetachedPayload can be used to both sign or verify a JWS message with a
// detached payload.
//
// When this option is used for `jws.Sign()`, the first parameter (normally the payload)
// must be set to `nil`.
//
// If you have to verify using this option, you should know exactly how and why this works.
func WithDetachedPayload(v []byte) SignVerifyOption {
	return &signVerifyOption{option.New(identDetachedPayload{}, v)}
}

// WithFetchWhitelist specifies the whitelist object to be passed
// to `jwk.Fetch()` when `jws.VerifyAuto()` is used. If you do not
// specify a whitelist, `jws.VerifyAuto()` will ALWAYS fail.
//
// This option is ignored if WithJWKSetFetcher is specified.
func WithFetchWhitelist(wl jwk.Whitelist) VerifyOption {
	return &verifyOption{option.New(identFetchWhitelist{}, wl)}
}

// WithFetchBackoff specifies the backoff.Policy object to be passed
// to `jwk.Fetch()` when `jws.VerifyAuto()` is used.
//
// This option is ignored if WithJWKSetFetcher is specified.
func WithFetchBackoff(b backoff.Policy) VerifyOption {
	return &verifyOption{option.New(identFetchBackoff{}, b)}
}

// WithHTTPClient specifies the *http.Client object to be passed
// to `jwk.Fetch()` when `jws.VerifyAuto()` is used.
//
// This option is ignored if WithJWKSetFetcher is specified.
func WithHTTPClient(httpcl *http.Client) VerifyOption {
	return &verifyOption{option.New(identHTTPClient{}, httpcl)}
}

// WithJWKSetFetcher specifies the JWKSetFetcher object to be
// used when `jws.VerifyAuto()`, for example, to use `jwk.AutoRefetch`
// instead of the default `jwk.Fetch()`
func WithJWKSetFetcher(f JWKSetFetcher) VerifyOption {
	return &verifyOption{option.New(identJWKSetFetcher{}, f)}
}
