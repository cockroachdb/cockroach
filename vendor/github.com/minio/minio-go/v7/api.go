/*
 * MinIO Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2015-2018 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package minio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httputil"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	md5simd "github.com/minio/md5-simd"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/minio-go/v7/pkg/signer"
	"golang.org/x/net/publicsuffix"
)

// Client implements Amazon S3 compatible methods.
type Client struct {
	//  Standard options.

	// Parsed endpoint url provided by the user.
	endpointURL *url.URL

	// Holds various credential providers.
	credsProvider *credentials.Credentials

	// Custom signerType value overrides all credentials.
	overrideSignerType credentials.SignatureType

	// User supplied.
	appInfo struct {
		appName    string
		appVersion string
	}

	// Indicate whether we are using https or not
	secure bool

	// Needs allocation.
	httpClient     *http.Client
	bucketLocCache *bucketLocationCache

	// Advanced functionality.
	isTraceEnabled  bool
	traceErrorsOnly bool
	traceOutput     io.Writer

	// S3 specific accelerated endpoint.
	s3AccelerateEndpoint string

	// Region endpoint
	region string

	// Random seed.
	random *rand.Rand

	// lookup indicates type of url lookup supported by server. If not specified,
	// default to Auto.
	lookup BucketLookupType

	// Factory for MD5 hash functions.
	md5Hasher    func() md5simd.Hasher
	sha256Hasher func() md5simd.Hasher

	healthStatus int32
}

// Options for New method
type Options struct {
	Creds        *credentials.Credentials
	Secure       bool
	Transport    http.RoundTripper
	Region       string
	BucketLookup BucketLookupType

	// Custom hash routines. Leave nil to use standard.
	CustomMD5    func() md5simd.Hasher
	CustomSHA256 func() md5simd.Hasher
}

// Global constants.
const (
	libraryName    = "minio-go"
	libraryVersion = "v7.0.21"
)

// User Agent should always following the below style.
// Please open an issue to discuss any new changes here.
//
//       MinIO (OS; ARCH) LIB/VER APP/VER
const (
	libraryUserAgentPrefix = "MinIO (" + runtime.GOOS + "; " + runtime.GOARCH + ") "
	libraryUserAgent       = libraryUserAgentPrefix + libraryName + "/" + libraryVersion
)

// BucketLookupType is type of url lookup supported by server.
type BucketLookupType int

// Different types of url lookup supported by the server.Initialized to BucketLookupAuto
const (
	BucketLookupAuto BucketLookupType = iota
	BucketLookupDNS
	BucketLookupPath
)

// New - instantiate minio client with options
func New(endpoint string, opts *Options) (*Client, error) {
	if opts == nil {
		return nil, errors.New("no options provided")
	}
	clnt, err := privateNew(endpoint, opts)
	if err != nil {
		return nil, err
	}
	// Google cloud storage should be set to signature V2, force it if not.
	if s3utils.IsGoogleEndpoint(*clnt.endpointURL) {
		clnt.overrideSignerType = credentials.SignatureV2
	}
	// If Amazon S3 set to signature v4.
	if s3utils.IsAmazonEndpoint(*clnt.endpointURL) {
		clnt.overrideSignerType = credentials.SignatureV4
	}

	return clnt, nil
}

// EndpointURL returns the URL of the S3 endpoint.
func (c *Client) EndpointURL() *url.URL {
	endpoint := *c.endpointURL // copy to prevent callers from modifying internal state
	return &endpoint
}

// lockedRandSource provides protected rand source, implements rand.Source interface.
type lockedRandSource struct {
	lk  sync.Mutex
	src rand.Source
}

// Int63 returns a non-negative pseudo-random 63-bit integer as an int64.
func (r *lockedRandSource) Int63() (n int64) {
	r.lk.Lock()
	n = r.src.Int63()
	r.lk.Unlock()
	return
}

// Seed uses the provided seed value to initialize the generator to a
// deterministic state.
func (r *lockedRandSource) Seed(seed int64) {
	r.lk.Lock()
	r.src.Seed(seed)
	r.lk.Unlock()
}

func privateNew(endpoint string, opts *Options) (*Client, error) {
	// construct endpoint.
	endpointURL, err := getEndpointURL(endpoint, opts.Secure)
	if err != nil {
		return nil, err
	}

	// Initialize cookies to preserve server sent cookies if any and replay
	// them upon each request.
	jar, err := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	if err != nil {
		return nil, err
	}

	// instantiate new Client.
	clnt := new(Client)

	// Save the credentials.
	clnt.credsProvider = opts.Creds

	// Remember whether we are using https or not
	clnt.secure = opts.Secure

	// Save endpoint URL, user agent for future uses.
	clnt.endpointURL = endpointURL

	transport := opts.Transport
	if transport == nil {
		transport, err = DefaultTransport(opts.Secure)
		if err != nil {
			return nil, err
		}
	}

	// Instantiate http client and bucket location cache.
	clnt.httpClient = &http.Client{
		Jar:       jar,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// Sets custom region, if region is empty bucket location cache is used automatically.
	if opts.Region == "" {
		opts.Region = s3utils.GetRegionFromURL(*clnt.endpointURL)
	}
	clnt.region = opts.Region

	// Instantiate bucket location cache.
	clnt.bucketLocCache = newBucketLocationCache()

	// Introduce a new locked random seed.
	clnt.random = rand.New(&lockedRandSource{src: rand.NewSource(time.Now().UTC().UnixNano())})

	// Add default md5 hasher.
	clnt.md5Hasher = opts.CustomMD5
	clnt.sha256Hasher = opts.CustomSHA256
	if clnt.md5Hasher == nil {
		clnt.md5Hasher = newMd5Hasher
	}
	if clnt.sha256Hasher == nil {
		clnt.sha256Hasher = newSHA256Hasher
	}
	// Sets bucket lookup style, whether server accepts DNS or Path lookup. Default is Auto - determined
	// by the SDK. When Auto is specified, DNS lookup is used for Amazon/Google cloud endpoints and Path for all other endpoints.
	clnt.lookup = opts.BucketLookup

	// healthcheck is not initialized
	clnt.healthStatus = unknown

	// Return.
	return clnt, nil
}

// SetAppInfo - add application details to user agent.
func (c *Client) SetAppInfo(appName string, appVersion string) {
	// if app name and version not set, we do not set a new user agent.
	if appName != "" && appVersion != "" {
		c.appInfo.appName = appName
		c.appInfo.appVersion = appVersion
	}
}

// TraceOn - enable HTTP tracing.
func (c *Client) TraceOn(outputStream io.Writer) {
	// if outputStream is nil then default to os.Stdout.
	if outputStream == nil {
		outputStream = os.Stdout
	}
	// Sets a new output stream.
	c.traceOutput = outputStream

	// Enable tracing.
	c.isTraceEnabled = true
}

// TraceErrorsOnlyOn - same as TraceOn, but only errors will be traced.
func (c *Client) TraceErrorsOnlyOn(outputStream io.Writer) {
	c.TraceOn(outputStream)
	c.traceErrorsOnly = true
}

// TraceErrorsOnlyOff - Turns off the errors only tracing and everything will be traced after this call.
// If all tracing needs to be turned off, call TraceOff().
func (c *Client) TraceErrorsOnlyOff() {
	c.traceErrorsOnly = false
}

// TraceOff - disable HTTP tracing.
func (c *Client) TraceOff() {
	// Disable tracing.
	c.isTraceEnabled = false
	c.traceErrorsOnly = false
}

// SetS3TransferAccelerate - turns s3 accelerated endpoint on or off for all your
// requests. This feature is only specific to S3 for all other endpoints this
// function does nothing. To read further details on s3 transfer acceleration
// please vist -
// http://docs.aws.amazon.com/AmazonS3/latest/dev/transfer-acceleration.html
func (c *Client) SetS3TransferAccelerate(accelerateEndpoint string) {
	if s3utils.IsAmazonEndpoint(*c.endpointURL) {
		c.s3AccelerateEndpoint = accelerateEndpoint
	}
}

// Hash materials provides relevant initialized hash algo writers
// based on the expected signature type.
//
//  - For signature v4 request if the connection is insecure compute only sha256.
//  - For signature v4 request if the connection is secure compute only md5.
//  - For anonymous request compute md5.
func (c *Client) hashMaterials(isMd5Requested bool) (hashAlgos map[string]md5simd.Hasher, hashSums map[string][]byte) {
	hashSums = make(map[string][]byte)
	hashAlgos = make(map[string]md5simd.Hasher)
	if c.overrideSignerType.IsV4() {
		if c.secure {
			hashAlgos["md5"] = c.md5Hasher()
		} else {
			hashAlgos["sha256"] = c.sha256Hasher()
		}
	} else {
		if c.overrideSignerType.IsAnonymous() {
			hashAlgos["md5"] = c.md5Hasher()
		}
	}
	if isMd5Requested {
		hashAlgos["md5"] = c.md5Hasher()
	}
	return hashAlgos, hashSums
}

const (
	unknown = -1
	offline = 0
	online  = 1
)

// IsOnline returns true if healthcheck enabled and client is online
func (c *Client) IsOnline() bool {
	return !c.IsOffline()
}

// sets online healthStatus to offline
func (c *Client) markOffline() {
	atomic.CompareAndSwapInt32(&c.healthStatus, online, offline)
}

// IsOffline returns true if healthcheck enabled and client is offline
func (c *Client) IsOffline() bool {
	return atomic.LoadInt32(&c.healthStatus) == offline
}

// HealthCheck starts a healthcheck to see if endpoint is up. Returns a context cancellation function
// and and error if health check is already started
func (c *Client) HealthCheck(hcDuration time.Duration) (context.CancelFunc, error) {
	if atomic.LoadInt32(&c.healthStatus) == online {
		return nil, fmt.Errorf("health check is running")
	}
	if hcDuration < 1*time.Second {
		return nil, fmt.Errorf("health check duration should be atleast 1 second")
	}
	ctx, cancelFn := context.WithCancel(context.Background())
	atomic.StoreInt32(&c.healthStatus, online)
	probeBucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "probe-health-")
	go func(duration time.Duration) {
		timer := time.NewTimer(duration)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				atomic.StoreInt32(&c.healthStatus, unknown)
				return
			case <-timer.C:
				timer.Reset(duration)
				// Do health check the first time and ONLY if the connection is marked offline
				if c.IsOffline() {
					gctx, gcancel := context.WithTimeout(context.Background(), 3*time.Second)
					_, err := c.getBucketLocation(gctx, probeBucketName)
					gcancel()
					if IsNetworkOrHostDown(err, false) {
						// Still network errors do not need to do anything.
						continue
					}
					switch ToErrorResponse(err).Code {
					case "NoSuchBucket", "AccessDenied", "":
						atomic.CompareAndSwapInt32(&c.healthStatus, offline, online)
					}
				}
			}
		}
	}(hcDuration)
	return cancelFn, nil
}

// requestMetadata - is container for all the values to make a request.
type requestMetadata struct {
	// If set newRequest presigns the URL.
	presignURL bool

	// User supplied.
	bucketName         string
	objectName         string
	queryValues        url.Values
	customHeader       http.Header
	extraPresignHeader http.Header
	expires            int64

	// Generated by our internal code.
	bucketLocation   string
	contentBody      io.Reader
	contentLength    int64
	contentMD5Base64 string // carries base64 encoded md5sum
	contentSHA256Hex string // carries hex encoded sha256sum
}

// dumpHTTP - dump HTTP request and response.
func (c *Client) dumpHTTP(req *http.Request, resp *http.Response) error {
	// Starts http dump.
	_, err := fmt.Fprintln(c.traceOutput, "---------START-HTTP---------")
	if err != nil {
		return err
	}

	// Filter out Signature field from Authorization header.
	origAuth := req.Header.Get("Authorization")
	if origAuth != "" {
		req.Header.Set("Authorization", redactSignature(origAuth))
	}

	// Only display request header.
	reqTrace, err := httputil.DumpRequestOut(req, false)
	if err != nil {
		return err
	}

	// Write request to trace output.
	_, err = fmt.Fprint(c.traceOutput, string(reqTrace))
	if err != nil {
		return err
	}

	// Only display response header.
	var respTrace []byte

	// For errors we make sure to dump response body as well.
	if resp.StatusCode != http.StatusOK &&
		resp.StatusCode != http.StatusPartialContent &&
		resp.StatusCode != http.StatusNoContent {
		respTrace, err = httputil.DumpResponse(resp, true)
		if err != nil {
			return err
		}
	} else {
		respTrace, err = httputil.DumpResponse(resp, false)
		if err != nil {
			return err
		}
	}

	// Write response to trace output.
	_, err = fmt.Fprint(c.traceOutput, strings.TrimSuffix(string(respTrace), "\r\n"))
	if err != nil {
		return err
	}

	// Ends the http dump.
	_, err = fmt.Fprintln(c.traceOutput, "---------END-HTTP---------")
	if err != nil {
		return err
	}

	// Returns success.
	return nil
}

// do - execute http request.
func (c *Client) do(req *http.Request) (resp *http.Response, err error) {
	defer func() {
		if IsNetworkOrHostDown(err, false) {
			c.markOffline()
		}
	}()

	resp, err = c.httpClient.Do(req)
	if err != nil {
		// Handle this specifically for now until future Golang versions fix this issue properly.
		if urlErr, ok := err.(*url.Error); ok {
			if strings.Contains(urlErr.Err.Error(), "EOF") {
				return nil, &url.Error{
					Op:  urlErr.Op,
					URL: urlErr.URL,
					Err: errors.New("Connection closed by foreign host " + urlErr.URL + ". Retry again."),
				}
			}
		}
		return nil, err
	}

	// Response cannot be non-nil, report error if thats the case.
	if resp == nil {
		msg := "Response is empty. " + reportIssue
		return nil, errInvalidArgument(msg)
	}

	// If trace is enabled, dump http request and response,
	// except when the traceErrorsOnly enabled and the response's status code is ok
	if c.isTraceEnabled && !(c.traceErrorsOnly && resp.StatusCode == http.StatusOK) {
		err = c.dumpHTTP(req, resp)
		if err != nil {
			return nil, err
		}
	}

	return resp, nil
}

// List of success status.
var successStatus = []int{
	http.StatusOK,
	http.StatusNoContent,
	http.StatusPartialContent,
}

// executeMethod - instantiates a given method, and retries the
// request upon any error up to maxRetries attempts in a binomially
// delayed manner using a standard back off algorithm.
func (c *Client) executeMethod(ctx context.Context, method string, metadata requestMetadata) (res *http.Response, err error) {
	if c.IsOffline() {
		return nil, errors.New(c.endpointURL.String() + " is offline.")
	}

	var retryable bool       // Indicates if request can be retried.
	var bodySeeker io.Seeker // Extracted seeker from io.Reader.
	var reqRetry = MaxRetry  // Indicates how many times we can retry the request

	if metadata.contentBody != nil {
		// Check if body is seekable then it is retryable.
		bodySeeker, retryable = metadata.contentBody.(io.Seeker)
		switch bodySeeker {
		case os.Stdin, os.Stdout, os.Stderr:
			retryable = false
		}
		// Retry only when reader is seekable
		if !retryable {
			reqRetry = 1
		}

		// Figure out if the body can be closed - if yes
		// we will definitely close it upon the function
		// return.
		bodyCloser, ok := metadata.contentBody.(io.Closer)
		if ok {
			defer bodyCloser.Close()
		}
	}

	// Create cancel context to control 'newRetryTimer' go routine.
	retryCtx, cancel := context.WithCancel(ctx)

	// Indicate to our routine to exit cleanly upon return.
	defer cancel()

	for range c.newRetryTimer(retryCtx, reqRetry, DefaultRetryUnit, DefaultRetryCap, MaxJitter) {
		// Retry executes the following function body if request has an
		// error until maxRetries have been exhausted, retry attempts are
		// performed after waiting for a given period of time in a
		// binomial fashion.
		if retryable {
			// Seek back to beginning for each attempt.
			if _, err = bodySeeker.Seek(0, 0); err != nil {
				// If seek failed, no need to retry.
				return nil, err
			}
		}

		// Instantiate a new request.
		var req *http.Request
		req, err = c.newRequest(ctx, method, metadata)
		if err != nil {
			errResponse := ToErrorResponse(err)
			if isS3CodeRetryable(errResponse.Code) {
				continue // Retry.
			}

			return nil, err
		}
		// Initiate the request.
		res, err = c.do(req)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}

			// Retry the request
			continue
		}

		// For any known successful http status, return quickly.
		for _, httpStatus := range successStatus {
			if httpStatus == res.StatusCode {
				return res, nil
			}
		}

		// Read the body to be saved later.
		errBodyBytes, err := ioutil.ReadAll(res.Body)
		// res.Body should be closed
		closeResponse(res)
		if err != nil {
			return nil, err
		}

		// Save the body.
		errBodySeeker := bytes.NewReader(errBodyBytes)
		res.Body = ioutil.NopCloser(errBodySeeker)

		// For errors verify if its retryable otherwise fail quickly.
		errResponse := ToErrorResponse(httpRespToErrorResponse(res, metadata.bucketName, metadata.objectName))

		// Save the body back again.
		errBodySeeker.Seek(0, 0) // Seek back to starting point.
		res.Body = ioutil.NopCloser(errBodySeeker)

		// Bucket region if set in error response and the error
		// code dictates invalid region, we can retry the request
		// with the new region.
		//
		// Additionally we should only retry if bucketLocation and custom
		// region is empty.
		if c.region == "" {
			switch errResponse.Code {
			case "AuthorizationHeaderMalformed":
				fallthrough
			case "InvalidRegion":
				fallthrough
			case "AccessDenied":
				if errResponse.Region == "" {
					// Region is empty we simply return the error.
					return res, err
				}
				// Region is not empty figure out a way to
				// handle this appropriately.
				if metadata.bucketName != "" {
					// Gather Cached location only if bucketName is present.
					if location, cachedOk := c.bucketLocCache.Get(metadata.bucketName); cachedOk && location != errResponse.Region {
						c.bucketLocCache.Set(metadata.bucketName, errResponse.Region)
						continue // Retry.
					}
				} else {
					// This is for ListBuckets() fallback.
					if errResponse.Region != metadata.bucketLocation {
						// Retry if the error response has a different region
						// than the request we just made.
						metadata.bucketLocation = errResponse.Region
						continue // Retry
					}
				}
			}
		}

		// Verify if error response code is retryable.
		if isS3CodeRetryable(errResponse.Code) {
			continue // Retry.
		}

		// Verify if http status code is retryable.
		if isHTTPStatusRetryable(res.StatusCode) {
			continue // Retry.
		}

		// For all other cases break out of the retry loop.
		break
	}

	// Return an error when retry is canceled or deadlined
	if e := retryCtx.Err(); e != nil {
		return nil, e
	}

	return res, err
}

// newRequest - instantiate a new HTTP request for a given method.
func (c *Client) newRequest(ctx context.Context, method string, metadata requestMetadata) (req *http.Request, err error) {
	// If no method is supplied default to 'POST'.
	if method == "" {
		method = http.MethodPost
	}

	location := metadata.bucketLocation
	if location == "" {
		if metadata.bucketName != "" {
			// Gather location only if bucketName is present.
			location, err = c.getBucketLocation(ctx, metadata.bucketName)
			if err != nil {
				return nil, err
			}
		}
		if location == "" {
			location = getDefaultLocation(*c.endpointURL, c.region)
		}
	}

	// Look if target url supports virtual host.
	// We explicitly disallow MakeBucket calls to not use virtual DNS style,
	// since the resolution may fail.
	isMakeBucket := (metadata.objectName == "" && method == http.MethodPut && len(metadata.queryValues) == 0)
	isVirtualHost := c.isVirtualHostStyleRequest(*c.endpointURL, metadata.bucketName) && !isMakeBucket

	// Construct a new target URL.
	targetURL, err := c.makeTargetURL(metadata.bucketName, metadata.objectName, location,
		isVirtualHost, metadata.queryValues)
	if err != nil {
		return nil, err
	}

	// Initialize a new HTTP request for the method.
	req, err = http.NewRequestWithContext(ctx, method, targetURL.String(), nil)
	if err != nil {
		return nil, err
	}

	// Get credentials from the configured credentials provider.
	value, err := c.credsProvider.Get()
	if err != nil {
		return nil, err
	}

	var (
		signerType      = value.SignerType
		accessKeyID     = value.AccessKeyID
		secretAccessKey = value.SecretAccessKey
		sessionToken    = value.SessionToken
	)

	// Custom signer set then override the behavior.
	if c.overrideSignerType != credentials.SignatureDefault {
		signerType = c.overrideSignerType
	}

	// If signerType returned by credentials helper is anonymous,
	// then do not sign regardless of signerType override.
	if value.SignerType == credentials.SignatureAnonymous {
		signerType = credentials.SignatureAnonymous
	}

	// Generate presign url if needed, return right here.
	if metadata.expires != 0 && metadata.presignURL {
		if signerType.IsAnonymous() {
			return nil, errInvalidArgument("Presigned URLs cannot be generated with anonymous credentials.")
		}
		if metadata.extraPresignHeader != nil {
			if signerType.IsV2() {
				return nil, errInvalidArgument("Extra signed headers for Presign with Signature V2 is not supported.")
			}
			for k, v := range metadata.extraPresignHeader {
				req.Header.Set(k, v[0])
			}
		}
		if signerType.IsV2() {
			// Presign URL with signature v2.
			req = signer.PreSignV2(*req, accessKeyID, secretAccessKey, metadata.expires, isVirtualHost)
		} else if signerType.IsV4() {
			// Presign URL with signature v4.
			req = signer.PreSignV4(*req, accessKeyID, secretAccessKey, sessionToken, location, metadata.expires)
		}
		return req, nil
	}

	// Set 'User-Agent' header for the request.
	c.setUserAgent(req)

	// Set all headers.
	for k, v := range metadata.customHeader {
		req.Header.Set(k, v[0])
	}

	// Go net/http notoriously closes the request body.
	// - The request Body, if non-nil, will be closed by the underlying Transport, even on errors.
	// This can cause underlying *os.File seekers to fail, avoid that
	// by making sure to wrap the closer as a nop.
	if metadata.contentLength == 0 {
		req.Body = nil
	} else {
		req.Body = ioutil.NopCloser(metadata.contentBody)
	}

	// Set incoming content-length.
	req.ContentLength = metadata.contentLength
	if req.ContentLength <= -1 {
		// For unknown content length, we upload using transfer-encoding: chunked.
		req.TransferEncoding = []string{"chunked"}
	}

	// set md5Sum for content protection.
	if len(metadata.contentMD5Base64) > 0 {
		req.Header.Set("Content-Md5", metadata.contentMD5Base64)
	}

	// For anonymous requests just return.
	if signerType.IsAnonymous() {
		return req, nil
	}

	switch {
	case signerType.IsV2():
		// Add signature version '2' authorization header.
		req = signer.SignV2(*req, accessKeyID, secretAccessKey, isVirtualHost)
	case metadata.objectName != "" && metadata.queryValues == nil && method == http.MethodPut && metadata.customHeader.Get("X-Amz-Copy-Source") == "" && !c.secure:
		// Streaming signature is used by default for a PUT object request. Additionally we also
		// look if the initialized client is secure, if yes then we don't need to perform
		// streaming signature.
		req = signer.StreamingSignV4(req, accessKeyID,
			secretAccessKey, sessionToken, location, metadata.contentLength, time.Now().UTC())
	default:
		// Set sha256 sum for signature calculation only with signature version '4'.
		shaHeader := unsignedPayload
		if metadata.contentSHA256Hex != "" {
			shaHeader = metadata.contentSHA256Hex
		}
		req.Header.Set("X-Amz-Content-Sha256", shaHeader)

		// Add signature version '4' authorization header.
		req = signer.SignV4(*req, accessKeyID, secretAccessKey, sessionToken, location)
	}

	// Return request.
	return req, nil
}

// set User agent.
func (c *Client) setUserAgent(req *http.Request) {
	req.Header.Set("User-Agent", libraryUserAgent)
	if c.appInfo.appName != "" && c.appInfo.appVersion != "" {
		req.Header.Set("User-Agent", libraryUserAgent+" "+c.appInfo.appName+"/"+c.appInfo.appVersion)
	}
}

// makeTargetURL make a new target url.
func (c *Client) makeTargetURL(bucketName, objectName, bucketLocation string, isVirtualHostStyle bool, queryValues url.Values) (*url.URL, error) {
	host := c.endpointURL.Host
	// For Amazon S3 endpoint, try to fetch location based endpoint.
	if s3utils.IsAmazonEndpoint(*c.endpointURL) {
		if c.s3AccelerateEndpoint != "" && bucketName != "" {
			// http://docs.aws.amazon.com/AmazonS3/latest/dev/transfer-acceleration.html
			// Disable transfer acceleration for non-compliant bucket names.
			if strings.Contains(bucketName, ".") {
				return nil, errTransferAccelerationBucket(bucketName)
			}
			// If transfer acceleration is requested set new host.
			// For more details about enabling transfer acceleration read here.
			// http://docs.aws.amazon.com/AmazonS3/latest/dev/transfer-acceleration.html
			host = c.s3AccelerateEndpoint
		} else {
			// Do not change the host if the endpoint URL is a FIPS S3 endpoint or a S3 PrivateLink interface endpoint
			if !s3utils.IsAmazonFIPSEndpoint(*c.endpointURL) && !s3utils.IsAmazonPrivateLinkEndpoint(*c.endpointURL) {
				// Fetch new host based on the bucket location.
				host = getS3Endpoint(bucketLocation)
			}
		}
	}

	// Save scheme.
	scheme := c.endpointURL.Scheme

	// Strip port 80 and 443 so we won't send these ports in Host header.
	// The reason is that browsers and curl automatically remove :80 and :443
	// with the generated presigned urls, then a signature mismatch error.
	if h, p, err := net.SplitHostPort(host); err == nil {
		if scheme == "http" && p == "80" || scheme == "https" && p == "443" {
			host = h
		}
	}

	urlStr := scheme + "://" + host + "/"
	// Make URL only if bucketName is available, otherwise use the
	// endpoint URL.
	if bucketName != "" {
		// If endpoint supports virtual host style use that always.
		// Currently only S3 and Google Cloud Storage would support
		// virtual host style.
		if isVirtualHostStyle {
			urlStr = scheme + "://" + bucketName + "." + host + "/"
			if objectName != "" {
				urlStr += s3utils.EncodePath(objectName)
			}
		} else {
			// If not fall back to using path style.
			urlStr = urlStr + bucketName + "/"
			if objectName != "" {
				urlStr += s3utils.EncodePath(objectName)
			}
		}
	}

	// If there are any query values, add them to the end.
	if len(queryValues) > 0 {
		urlStr = urlStr + "?" + s3utils.QueryEncode(queryValues)
	}

	return url.Parse(urlStr)
}

// returns true if virtual hosted style requests are to be used.
func (c *Client) isVirtualHostStyleRequest(url url.URL, bucketName string) bool {
	if bucketName == "" {
		return false
	}

	if c.lookup == BucketLookupDNS {
		return true
	}
	if c.lookup == BucketLookupPath {
		return false
	}

	// default to virtual only for Amazon/Google  storage. In all other cases use
	// path style requests
	return s3utils.IsVirtualHostSupported(url, bucketName)
}
