package jwk

import (
	"context"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/lestrrat-go/backoff/v2"
	"github.com/lestrrat-go/httpcc"
	"github.com/pkg/errors"
)

// AutoRefresh is a container that keeps track of jwk.Set object by their source URLs.
// The jwk.Set objects are refreshed automatically behind the scenes.
//
// Before retrieving the jwk.Set objects, the user must pre-register the
// URLs they intend to use by calling `Configure()`
//
//  ar := jwk.NewAutoRefresh(ctx)
//  ar.Configure(url, options...)
//
// Once registered, you can call `Fetch()` to retrieve the jwk.Set object.
//
// All JWKS objects that are retrieved via the auto-fetch mechanism should be
// treated read-only, as they are shared among the consumers and this object.
type AutoRefresh struct {
	errSink      chan AutoRefreshError
	cache        map[string]Set
	configureCh  chan struct{}
	removeCh     chan removeReq
	fetching     map[string]chan struct{}
	muErrSink    sync.Mutex
	muCache      sync.RWMutex
	muFetching   sync.Mutex
	muRegistry   sync.RWMutex
	registry     map[string]*target
	resetTimerCh chan *resetTimerReq
}

type target struct {
	// The backoff policy to use when fetching the JWKS fails
	backoff backoff.Policy

	// The HTTP client to use. The user may opt to use a client which is
	// aware of HTTP caching, or one that goes through a proxy
	httpcl HTTPClient

	// Interval between refreshes are calculated two ways.
	// 1) You can set an explicit refresh interval by using WithRefreshInterval().
	//    In this mode, it doesn't matter what the HTTP response says in its
	//    Cache-Control or Expires headers
	// 2) You can let us calculate the time-to-refresh based on the key's
	//	  Cache-Control or Expires headers.
	//    First, the user provides us the absolute minimum interval before
	//    refreshes. We will never check for refreshes before this specified
	//    amount of time.
	//
	//    Next, max-age directive in the Cache-Control header is consulted.
	//    If `max-age` is not present, we skip the following section, and
	//    proceed to the next option.
	//    If `max-age > user-supplied minimum interval`, then we use the max-age,
	//    otherwise the user-supplied minimum interval is used.
	//
	//    Next, the value specified in Expires header is consulted.
	//    If the header is not present, we skip the following seciont and
	//    proceed to the next option.
	//    We take the time until expiration `expires - time.Now()`, and
	//	  if `time-until-expiration > user-supplied minimum interval`, then
	//    we use the expires value, otherwise the user-supplied minimum interval is used.
	//
	//    If all of the above fails, we used the user-supplied minimum interval
	refreshInterval    *time.Duration
	minRefreshInterval time.Duration

	url string

	// The timer for refreshing the keyset. should not be set by anyone
	// other than the refreshing goroutine
	timer *time.Timer

	// Semaphore to limit the number of concurrent refreshes in the background
	sem chan struct{}

	// for debugging, snapshoting
	lastRefresh time.Time
	nextRefresh time.Time

	wl           Whitelist
	parseOptions []ParseOption
}

type resetTimerReq struct {
	t *target
	d time.Duration
}

// NewAutoRefresh creates a container that keeps track of JWKS objects which
// are automatically refreshed.
//
// The context object in the argument controls the life-span of the
// auto-refresh worker. If you are using this in a long running process, this
// should mostly be set to a context that ends when the main loop/part of your
// program exits:
//
// func MainLoop() {
//   ctx, cancel := context.WithCancel(context.Background())
//   defer cancel()
//   ar := jwk.AutoRefresh(ctx)
//   for ... {
//     ...
//   }
// }
func NewAutoRefresh(ctx context.Context) *AutoRefresh {
	af := &AutoRefresh{
		cache:        make(map[string]Set),
		configureCh:  make(chan struct{}),
		removeCh:     make(chan removeReq),
		fetching:     make(map[string]chan struct{}),
		registry:     make(map[string]*target),
		resetTimerCh: make(chan *resetTimerReq),
	}
	go af.refreshLoop(ctx)
	return af
}

func (af *AutoRefresh) getCached(url string) (Set, bool) {
	af.muCache.RLock()
	ks, ok := af.cache[url]
	af.muCache.RUnlock()
	if ok {
		return ks, true
	}
	return nil, false
}

type removeReq struct {
	replyCh chan error
	url     string
}

// Remove removes `url` from the list of urls being watched by jwk.AutoRefresh.
// If the url is not already registered, returns an error.
func (af *AutoRefresh) Remove(url string) error {
	ch := make(chan error)
	af.removeCh <- removeReq{replyCh: ch, url: url}
	return <-ch
}

// Configure registers the url to be controlled by AutoRefresh, and also
// sets any options associated to it.
//
// Note that options are treated as a whole -- you can't just update
// one value. For example, if you did:
//
//   ar.Configure(url, jwk.WithHTTPClient(...))
//   ar.Configure(url, jwk.WithRefreshInterval(...))
// The the end result is that `url` is ONLY associated with the options
// given in the second call to `Configure()`, i.e. `jwk.WithRefreshInterval`.
// The other unspecified options, including the HTTP client, is set to
// their default values.
//
// Configuration must propagate between goroutines, and therefore are
// not atomic (But changes should be felt "soon enough" for practical
// purposes)
func (af *AutoRefresh) Configure(url string, options ...AutoRefreshOption) {
	var httpcl HTTPClient = http.DefaultClient
	var hasRefreshInterval bool
	var refreshInterval time.Duration
	var wl Whitelist
	var parseOptions []ParseOption
	minRefreshInterval := time.Hour
	bo := backoff.Null()
	for _, option := range options {
		if v, ok := option.(ParseOption); ok {
			parseOptions = append(parseOptions, v)
			continue
		}

		//nolint:forcetypeassert
		switch option.Ident() {
		case identFetchBackoff{}:
			bo = option.Value().(backoff.Policy)
		case identRefreshInterval{}:
			refreshInterval = option.Value().(time.Duration)
			hasRefreshInterval = true
		case identMinRefreshInterval{}:
			minRefreshInterval = option.Value().(time.Duration)
		case identHTTPClient{}:
			httpcl = option.Value().(HTTPClient)
		case identFetchWhitelist{}:
			wl = option.Value().(Whitelist)
		}
	}

	af.muRegistry.Lock()
	t, ok := af.registry[url]
	if ok {
		if t.httpcl != httpcl {
			t.httpcl = httpcl
		}

		if t.minRefreshInterval != minRefreshInterval {
			t.minRefreshInterval = minRefreshInterval
		}

		if t.refreshInterval != nil {
			if !hasRefreshInterval {
				t.refreshInterval = nil
			} else if *t.refreshInterval != refreshInterval {
				*t.refreshInterval = refreshInterval
			}
		} else {
			if hasRefreshInterval {
				t.refreshInterval = &refreshInterval
			}
		}

		if t.wl != wl {
			t.wl = wl
		}

		t.parseOptions = parseOptions
	} else {
		t = &target{
			backoff:            bo,
			httpcl:             httpcl,
			minRefreshInterval: minRefreshInterval,
			url:                url,
			sem:                make(chan struct{}, 1),
			// This is a placeholder timer so we can call Reset() on it later
			// Make it sufficiently in the future so that we don't have bogus
			// events firing
			timer:        time.NewTimer(24 * time.Hour),
			wl:           wl,
			parseOptions: parseOptions,
		}
		if hasRefreshInterval {
			t.refreshInterval = &refreshInterval
		}

		// Record this in the registry
		af.registry[url] = t
	}
	af.muRegistry.Unlock()

	// Tell the backend to reconfigure itself
	af.configureCh <- struct{}{}
}

func (af *AutoRefresh) releaseFetching(url string) {
	// first delete the entry from the map, then close the channel or
	// otherwise we may end up getting multiple groutines doing the fetch
	af.muFetching.Lock()
	fetchingCh, ok := af.fetching[url]
	if !ok {
		// Juuuuuuust in case. But shouldn't happen
		af.muFetching.Unlock()
		return
	}
	delete(af.fetching, url)
	close(fetchingCh)
	af.muFetching.Unlock()
}

// IsRegistered checks if `url` is registered already.
func (af *AutoRefresh) IsRegistered(url string) bool {
	_, ok := af.getRegistered(url)
	return ok
}

// Fetch returns a jwk.Set from the given url.
func (af *AutoRefresh) getRegistered(url string) (*target, bool) {
	af.muRegistry.RLock()
	t, ok := af.registry[url]
	af.muRegistry.RUnlock()
	return t, ok
}

// Fetch returns a jwk.Set from the given url.
//
// If it has previously been fetched, then a cached value is returned.
//
// If this the first time `url` was requested, an HTTP request will be
// sent, synchronously.
//
// When accessed via multiple goroutines concurrently, and the cache
// has not been populated yet, only the first goroutine is
// allowed to perform the initialization (HTTP fetch and cache population).
// All other goroutines will be blocked until the operation is completed.
//
// DO NOT modify the jwk.Set object returned by this method, as the
// objects are shared among all consumers and the backend goroutine
func (af *AutoRefresh) Fetch(ctx context.Context, url string) (Set, error) {
	if _, ok := af.getRegistered(url); !ok {
		return nil, errors.Errorf(`url %s must be configured using "Configure()" first`, url)
	}

	ks, found := af.getCached(url)
	if found {
		return ks, nil
	}

	return af.refresh(ctx, url)
}

// Refresh is the same as Fetch(), except that HTTP fetching is done synchronously.
//
// This is useful when you want to force an HTTP fetch instead of waiting
// for the background goroutine to do it, for example when you want to
// make sure the AutoRefresh cache is warmed up before starting your main loop
func (af *AutoRefresh) Refresh(ctx context.Context, url string) (Set, error) {
	if _, ok := af.getRegistered(url); !ok {
		return nil, errors.Errorf(`url %s must be configured using "Configure()" first`, url)
	}

	return af.refresh(ctx, url)
}

func (af *AutoRefresh) refresh(ctx context.Context, url string) (Set, error) {
	// To avoid a thundering herd, only one goroutine per url may enter into this
	// initial fetch phase.
	af.muFetching.Lock()
	fetchingCh, fetching := af.fetching[url]
	// unlock happens in each of the if/else clauses because we need to perform
	// the channel initialization when there is no channel present
	if fetching {
		af.muFetching.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-fetchingCh:
		}
	} else {
		fetchingCh = make(chan struct{})
		af.fetching[url] = fetchingCh
		af.muFetching.Unlock()

		// Register a cleanup handler, to make sure we always
		defer af.releaseFetching(url)

		// The first time around, we need to fetch the keyset
		if err := af.doRefreshRequest(ctx, url, false); err != nil {
			return nil, errors.Wrapf(err, `failed to fetch resource pointed by %s`, url)
		}
	}

	// the cache should now be populated
	ks, ok := af.getCached(url)
	if !ok {
		return nil, errors.New("cache was not populated after explicit refresh")
	}

	return ks, nil
}

// Keeps looping, while refreshing the KeySet.
func (af *AutoRefresh) refreshLoop(ctx context.Context) {
	// reflect.Select() is slow IF we are executing it over and over
	// in a very fast iteration, but we assume here that refreshes happen
	// seldom enough that being able to call one `select{}` with multiple
	// targets / channels outweighs the speed penalty of using reflect.
	//
	const (
		ctxDoneIdx = iota
		configureIdx
		resetTimerIdx
		removeIdx
		baseSelcasesLen
	)

	baseSelcases := make([]reflect.SelectCase, baseSelcasesLen)
	baseSelcases[ctxDoneIdx] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	}
	baseSelcases[configureIdx] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(af.configureCh),
	}
	baseSelcases[resetTimerIdx] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(af.resetTimerCh),
	}
	baseSelcases[removeIdx] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(af.removeCh),
	}

	var targets []*target
	var selcases []reflect.SelectCase
	for {
		// It seems silly, but it's much easier to keep track of things
		// if we re-build the select cases every iteration

		af.muRegistry.RLock()
		if cap(targets) < len(af.registry) {
			targets = make([]*target, 0, len(af.registry))
		} else {
			targets = targets[:0]
		}

		if cap(selcases) < len(af.registry) {
			selcases = make([]reflect.SelectCase, 0, len(af.registry)+baseSelcasesLen)
		} else {
			selcases = selcases[:0]
		}
		selcases = append(selcases, baseSelcases...)

		for _, data := range af.registry {
			targets = append(targets, data)
			selcases = append(selcases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(data.timer.C),
			})
		}
		af.muRegistry.RUnlock()

		chosen, recv, recvOK := reflect.Select(selcases)
		switch chosen {
		case ctxDoneIdx:
			// <-ctx.Done(). Just bail out of this loop
			return
		case configureIdx:
			// <-configureCh. rebuild the select list from the registry.
			// since we're rebuilding everything for each iteration,
			// we just need to start the loop all over again
			continue
		case resetTimerIdx:
			// <-resetTimerCh. interrupt polling, and reset the timer on
			// a single target. this needs to be handled inside this select
			if !recvOK {
				continue
			}

			req := recv.Interface().(*resetTimerReq) //nolint:forcetypeassert
			t := req.t
			d := req.d
			if !t.timer.Stop() {
				select {
				case <-t.timer.C:
				default:
				}
			}
			t.timer.Reset(d)
		case removeIdx:
			// <-removeCh. remove the URL from future fetching
			//nolint:forcetypeassert
			req := recv.Interface().(removeReq)
			replyCh := req.replyCh
			url := req.url
			af.muRegistry.Lock()
			if _, ok := af.registry[url]; !ok {
				replyCh <- errors.Errorf(`invalid url %q (not registered)`, url)
			} else {
				delete(af.registry, url)
				replyCh <- nil
			}
			af.muRegistry.Unlock()
		default:
			// Do not fire a refresh in case the channel was closed.
			if !recvOK {
				continue
			}

			// Time to refresh a target
			t := targets[chosen-baseSelcasesLen]

			// Check if there are other goroutines still doing the refresh asynchronously.
			// This could happen if the refreshing goroutine is stuck on a backoff
			// waiting for the HTTP request to complete.
			select {
			case t.sem <- struct{}{}:
				// There can only be one refreshing goroutine
			default:
				continue
			}

			go func() {
				//nolint:errcheck
				af.doRefreshRequest(ctx, t.url, true)
				<-t.sem
			}()
		}
	}
}

func (af *AutoRefresh) doRefreshRequest(ctx context.Context, url string, enableBackoff bool) error {
	af.muRegistry.RLock()
	t, ok := af.registry[url]

	if !ok {
		af.muRegistry.RUnlock()
		return errors.Errorf(`url "%s" is not registered`, url)
	}

	// In case the refresh fails due to errors in fetching/parsing the JWKS,
	// we want to retry. Create a backoff object,
	parseOptions := t.parseOptions
	fetchOptions := []FetchOption{WithHTTPClient(t.httpcl)}
	if enableBackoff {
		fetchOptions = append(fetchOptions, WithFetchBackoff(t.backoff))
	}
	if t.wl != nil {
		fetchOptions = append(fetchOptions, WithFetchWhitelist(t.wl))
	}
	af.muRegistry.RUnlock()

	res, err := fetch(ctx, url, fetchOptions...)
	if err == nil {
		if res.StatusCode != http.StatusOK {
			// now, can there be a remote resource that responds with a status code
			// other than 200 and still be valid...? naaaaaaahhhhhh....
			err = errors.Errorf(`bad response status code (%d)`, res.StatusCode)
		} else {
			defer res.Body.Close()
			keyset, parseErr := ParseReader(res.Body, parseOptions...)
			if parseErr == nil {
				// Got a new key set. replace the keyset in the target
				af.muCache.Lock()
				af.cache[url] = keyset
				af.muCache.Unlock()
				af.muRegistry.RLock()
				nextInterval := calculateRefreshDuration(res, t.refreshInterval, t.minRefreshInterval)
				af.muRegistry.RUnlock()
				rtr := &resetTimerReq{
					t: t,
					d: nextInterval,
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case af.resetTimerCh <- rtr:
				}

				now := time.Now()
				af.muRegistry.Lock()
				t.lastRefresh = now.Local()
				t.nextRefresh = now.Add(nextInterval).Local()
				af.muRegistry.Unlock()
				return nil
			}
			err = parseErr
		}
	}

	// At this point if err != nil, we know that there was something wrong
	// in either the fetching or the parsing. Send this error to be processed,
	// but take the extra mileage to not block regular processing by
	// discarding the error if we fail to send it through the channel
	if err != nil {
		select {
		case af.errSink <- AutoRefreshError{Error: err, URL: url}:
		default:
		}
	}

	// We either failed to perform the HTTP GET, or we failed to parse the
	// JWK set. Even in case of errors, we don't delete the old key.
	// We persist the old key set, even if it may be stale so the user has something to work with
	// TODO: maybe this behavior should be customizable?

	// If we failed to get a single time, then queue another fetch in the future.
	rtr := &resetTimerReq{
		t: t,
		d: calculateRefreshDuration(res, t.refreshInterval, t.minRefreshInterval),
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case af.resetTimerCh <- rtr:
	}

	return err
}

// ErrorSink sets a channel to receive JWK fetch errors, if any.
// Only the errors that occurred *after* the channel was set  will be sent.
//
// The user is responsible for properly draining the channel. If the channel
// is not drained properly, errors will be discarded.
//
// To disable, set a nil channel.
func (af *AutoRefresh) ErrorSink(ch chan AutoRefreshError) {
	af.muErrSink.Lock()
	af.errSink = ch
	af.muErrSink.Unlock()
}

func calculateRefreshDuration(res *http.Response, refreshInterval *time.Duration, minRefreshInterval time.Duration) time.Duration {
	// This always has precedence
	if refreshInterval != nil {
		return *refreshInterval
	}

	if res != nil {
		if v := res.Header.Get(`Cache-Control`); v != "" {
			dir, err := httpcc.ParseResponse(v)
			if err == nil {
				maxAge, ok := dir.MaxAge()
				if ok {
					resDuration := time.Duration(maxAge) * time.Second
					if resDuration > minRefreshInterval {
						return resDuration
					}
					return minRefreshInterval
				}
				// fallthrough
			}
			// fallthrough
		}

		if v := res.Header.Get(`Expires`); v != "" {
			expires, err := http.ParseTime(v)
			if err == nil {
				resDuration := time.Until(expires)
				if resDuration > minRefreshInterval {
					return resDuration
				}
				return minRefreshInterval
			}
			// fallthrough
		}
	}

	// Previous fallthroughs are a little redandunt, but hey, it's all good.
	return minRefreshInterval
}

// TargetSnapshot is the structure returned by the Snapshot method.
// It contains information about a url that has been configured
// in AutoRefresh.
type TargetSnapshot struct {
	URL         string
	NextRefresh time.Time
	LastRefresh time.Time
}

func (af *AutoRefresh) Snapshot() <-chan TargetSnapshot {
	af.muRegistry.Lock()
	ch := make(chan TargetSnapshot, len(af.registry))
	for url, t := range af.registry {
		ch <- TargetSnapshot{
			URL:         url,
			NextRefresh: t.nextRefresh,
			LastRefresh: t.lastRefresh,
		}
	}
	af.muRegistry.Unlock()
	close(ch)
	return ch
}
