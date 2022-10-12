package jwk

import (
	"crypto"
	"time"

	"github.com/lestrrat-go/backoff/v2"
	"github.com/lestrrat-go/jwx/internal/json"
	"github.com/lestrrat-go/option"
)

type Option = option.Interface

type identHTTPClient struct{}
type identThumbprintHash struct{}
type identRefreshInterval struct{}
type identMinRefreshInterval struct{}
type identFetchBackoff struct{}
type identPEM struct{}
type identTypedField struct{}
type identLocalRegistry struct{}
type identFetchWhitelist struct{}
type identIgnoreParseError struct{}

// AutoRefreshOption is a type of Option that can be passed to the
// AutoRefresh object.
type AutoRefreshOption interface {
	Option
	autoRefreshOption()
}

type autoRefreshOption struct {
	Option
}

func (*autoRefreshOption) autoRefreshOption() {}

// FetchOption is a type of Option that can be passed to `jwk.Fetch()`
// FetchOption also implements the `AutoRefreshOption`, and thus can
// safely be passed to `(*jwk.AutoRefresh).Configure()`
type FetchOption interface {
	AutoRefreshOption
	fetchOption()
}

type fetchOption struct {
	Option
}

func (*fetchOption) autoRefreshOption() {}
func (*fetchOption) fetchOption()       {}

// ParseOption is a type of Option that can be passed to `jwk.Parse()`
// ParseOption also implmentsthe `ReadFileOPtion` and `AutoRefreshOption`,
// and thus safely be passed to `jwk.ReadFile` and `(*jwk.AutoRefresh).Configure()`
type ParseOption interface {
	ReadFileOption
	AutoRefreshOption
	parseOption()
}

type parseOption struct {
	Option
}

func (*parseOption) autoRefreshOption() {}
func (*parseOption) parseOption()       {}
func (*parseOption) readFileOption()    {}

// WithHTTPClient allows users to specify the "net/http".Client object that
// is used when fetching jwk.Set objects.
func WithHTTPClient(cl HTTPClient) FetchOption {
	return &fetchOption{option.New(identHTTPClient{}, cl)}
}

// WithFetchBackoff specifies the backoff policy to use when
// refreshing a JWKS from a remote server fails.
//
// This does not have any effect on initial `Fetch()`, or any of the `Refresh()` calls --
// the backoff is applied ONLY on the background refreshing goroutine.
func WithFetchBackoff(v backoff.Policy) FetchOption {
	return &fetchOption{option.New(identFetchBackoff{}, v)}
}

func WithThumbprintHash(h crypto.Hash) Option {
	return option.New(identThumbprintHash{}, h)
}

// WithRefreshInterval specifies the static interval between refreshes
// of jwk.Set objects controlled by jwk.AutoRefresh.
//
// Providing this option overrides the adaptive token refreshing based
// on Cache-Control/Expires header (and jwk.WithMinRefreshInterval),
// and refreshes will *always* happen in this interval.
func WithRefreshInterval(d time.Duration) AutoRefreshOption {
	return &autoRefreshOption{
		option.New(identRefreshInterval{}, d),
	}
}

// WithMinRefreshInterval specifies the minimum refresh interval to be used
// when using AutoRefresh. This value is ONLY used if you did not specify
// a user-supplied static refresh interval via `WithRefreshInterval`.
//
// This value is used as a fallback value when tokens are refreshed.
//
// When we fetch the key from a remote URL, we first look at the max-age
// directive from Cache-Control response header. If this value is present,
// we compare the max-age value and the value specified by this option
// and take the larger one.
//
// Next we check for the Expires header, and similarly if the header is
// present, we compare it against the value specified by this option,
// and take the larger one.
//
// Finally, if neither of the above headers are present, we use the
// value specified by this option as the next refresh timing
//
// If unspecified, the minimum refresh interval is 1 hour
func WithMinRefreshInterval(d time.Duration) AutoRefreshOption {
	return &autoRefreshOption{
		option.New(identMinRefreshInterval{}, d),
	}
}

// WithPEM specifies that the input to `Parse()` is a PEM encoded key.
func WithPEM(v bool) ParseOption {
	return &parseOption{
		option.New(identPEM{}, v),
	}
}

type typedFieldPair struct {
	Name  string
	Value interface{}
}

// WithTypedField allows a private field to be parsed into the object type of
// your choice. It works much like the RegisterCustomField, but the effect
// is only applicable to the jwt.Parse function call which receives this option.
//
// While this can be extremely useful, this option should be used with caution:
// There are many caveats that your entire team/user-base needs to be aware of,
// and therefore in general its use is discouraged. Only use it when you know
// what you are doing, and you document its use clearly for others.
//
// First and foremost, this is a "per-object" option. Meaning that given the same
// serialized format, it is possible to generate two objects whose internal
// representations may differ. That is, if you parse one _WITH_ the option,
// and the other _WITHOUT_, their internal representation may completely differ.
// This could potentially lead to problems.
//
// Second, specifying this option will slightly slow down the decoding process
// as it needs to consult multiple definitions sources (global and local), so
// be careful if you are decoding a large number of tokens, as the effects will stack up.
func WithTypedField(name string, object interface{}) ParseOption {
	return &parseOption{
		option.New(identTypedField{},
			typedFieldPair{Name: name, Value: object},
		),
	}
}

// This option is only available for internal code. Users don't get to play with it
func withLocalRegistry(r *json.Registry) ParseOption {
	return &parseOption{option.New(identLocalRegistry{}, r)}
}

// WithFetchWhitelist specifies the Whitelist object to use when
// fetching JWKs from a remote source. This option can be passed
// to both `jwk.Fetch()`, `jwk.NewAutoRefresh()`, and `(*jwk.AutoRefresh).Configure()`
func WithFetchWhitelist(w Whitelist) FetchOption {
	return &fetchOption{option.New(identFetchWhitelist{}, w)}
}

// WithIgnoreParseError is only applicable when used with `jwk.Parse()`
// (i.e. to parse JWK sets). If passed to `jwk.ParseKey()`, the function
// will return an error no matter what the input is.
//
// DO NOT USE WITHOUT EXHAUSTING ALL OTHER ROUTES FIRST.
//
// The option specifies that errors found during parsing of individual
// keys are ignored. For example, if you had keys A, B, C where B is
// invalid (e.g. it does not contain the required fields), then the
// resulting JWKS will contain keys A and C only.
//
// This options exists as an escape hatch for those times when a
// key in a JWKS that is irrelevant for your use case is causing
// your JWKS parsing to fail, and you want to get to the rest of the
// keys in the JWKS.
//
// Again, DO NOT USE unless you have exhausted all other routes.
// When you use this option, you will not be able to tell if you are
// using a faulty JWKS, except for when there are JSON syntax errors.
func WithIgnoreParseError(b bool) ParseOption {
	return &parseOption{option.New(identIgnoreParseError{}, b)}
}
