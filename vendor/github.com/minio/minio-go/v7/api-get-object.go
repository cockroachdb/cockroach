/*
 * MinIO Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2015-2020 MinIO, Inc.
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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"

	"github.com/minio/minio-go/v7/pkg/s3utils"
)

// GetObject wrapper function that accepts a request context
func (c *Client) GetObject(ctx context.Context, bucketName, objectName string, opts GetObjectOptions) (*Object, error) {
	// Input validation.
	if err := s3utils.CheckValidBucketName(bucketName); err != nil {
		return nil, err
	}
	if err := s3utils.CheckValidObjectName(objectName); err != nil {
		return nil, err
	}

	// Detect if snowball is server location we are talking to.
	var snowball bool
	if location, ok := c.bucketLocCache.Get(bucketName); ok {
		if location == "snowball" {
			snowball = true
		}
	}

	var (
		err        error
		httpReader io.ReadCloser
		objectInfo ObjectInfo
		totalRead  int
	)

	// Create request channel.
	reqCh := make(chan getRequest)
	// Create response channel.
	resCh := make(chan getResponse)
	// Create done channel.
	doneCh := make(chan struct{})

	// This routine feeds partial object data as and when the caller reads.
	go func() {
		defer close(reqCh)
		defer close(resCh)

		// Used to verify if etag of object has changed since last read.
		var etag string

		// Loop through the incoming control messages and read data.
		for {
			select {
			// When the done channel is closed exit our routine.
			case <-doneCh:
				// Close the http response body before returning.
				// This ends the connection with the server.
				if httpReader != nil {
					httpReader.Close()
				}
				return

			// Gather incoming request.
			case req := <-reqCh:
				// If this is the first request we may not need to do a getObject request yet.
				if req.isFirstReq {
					// First request is a Read/ReadAt.
					if req.isReadOp {
						// Differentiate between wanting the whole object and just a range.
						if req.isReadAt {
							// If this is a ReadAt request only get the specified range.
							// Range is set with respect to the offset and length of the buffer requested.
							// Do not set objectInfo from the first readAt request because it will not get
							// the whole object.
							opts.SetRange(req.Offset, req.Offset+int64(len(req.Buffer))-1)
						} else if req.Offset > 0 {
							opts.SetRange(req.Offset, 0)
						}
						httpReader, objectInfo, _, err = c.getObject(ctx, bucketName, objectName, opts)
						if err != nil {
							resCh <- getResponse{Error: err}
							return
						}
						etag = objectInfo.ETag
						// Read at least firstReq.Buffer bytes, if not we have
						// reached our EOF.
						size, err := readFull(httpReader, req.Buffer)
						totalRead += size
						if size > 0 && err == io.ErrUnexpectedEOF {
							if int64(size) < objectInfo.Size {
								// In situations when returned size
								// is less than the expected content
								// length set by the server, make sure
								// we return io.ErrUnexpectedEOF
								err = io.ErrUnexpectedEOF
							} else {
								// If an EOF happens after reading some but not
								// all the bytes ReadFull returns ErrUnexpectedEOF
								err = io.EOF
							}
						} else if size == 0 && err == io.EOF && objectInfo.Size > 0 {
							// Special cases when server writes more data
							// than the content-length, net/http response
							// body returns an error, instead of converting
							// it to io.EOF - return unexpected EOF.
							err = io.ErrUnexpectedEOF
						}
						// Send back the first response.
						resCh <- getResponse{
							objectInfo: objectInfo,
							Size:       size,
							Error:      err,
							didRead:    true,
						}
					} else {
						// First request is a Stat or Seek call.
						// Only need to run a StatObject until an actual Read or ReadAt request comes through.

						// Remove range header if already set, for stat Operations to get original file size.
						delete(opts.headers, "Range")
						objectInfo, err = c.StatObject(ctx, bucketName, objectName, StatObjectOptions(opts))
						if err != nil {
							resCh <- getResponse{
								Error: err,
							}
							// Exit the go-routine.
							return
						}
						etag = objectInfo.ETag
						// Send back the first response.
						resCh <- getResponse{
							objectInfo: objectInfo,
						}
					}
				} else if req.settingObjectInfo { // Request is just to get objectInfo.
					// Remove range header if already set, for stat Operations to get original file size.
					delete(opts.headers, "Range")
					// Check whether this is snowball
					// if yes do not use If-Match feature
					// it doesn't work.
					if etag != "" && !snowball {
						opts.SetMatchETag(etag)
					}
					objectInfo, err := c.StatObject(ctx, bucketName, objectName, StatObjectOptions(opts))
					if err != nil {
						resCh <- getResponse{
							Error: err,
						}
						// Exit the goroutine.
						return
					}
					// Send back the objectInfo.
					resCh <- getResponse{
						objectInfo: objectInfo,
					}
				} else {
					// Offset changes fetch the new object at an Offset.
					// Because the httpReader may not be set by the first
					// request if it was a stat or seek it must be checked
					// if the object has been read or not to only initialize
					// new ones when they haven't been already.
					// All readAt requests are new requests.
					if req.DidOffsetChange || !req.beenRead {
						// Check whether this is snowball
						// if yes do not use If-Match feature
						// it doesn't work.
						if etag != "" && !snowball {
							opts.SetMatchETag(etag)
						}
						if httpReader != nil {
							// Close previously opened http reader.
							httpReader.Close()
						}
						// If this request is a readAt only get the specified range.
						if req.isReadAt {
							// Range is set with respect to the offset and length of the buffer requested.
							opts.SetRange(req.Offset, req.Offset+int64(len(req.Buffer))-1)
						} else if req.Offset > 0 { // Range is set with respect to the offset.
							opts.SetRange(req.Offset, 0)
						}
						httpReader, objectInfo, _, err = c.getObject(ctx, bucketName, objectName, opts)
						if err != nil {
							resCh <- getResponse{
								Error: err,
							}
							return
						}
						totalRead = 0
					}

					// Read at least req.Buffer bytes, if not we have
					// reached our EOF.
					size, err := readFull(httpReader, req.Buffer)
					totalRead += size
					if size > 0 && err == io.ErrUnexpectedEOF {
						if int64(totalRead) < objectInfo.Size {
							// In situations when returned size
							// is less than the expected content
							// length set by the server, make sure
							// we return io.ErrUnexpectedEOF
							err = io.ErrUnexpectedEOF
						} else {
							// If an EOF happens after reading some but not
							// all the bytes ReadFull returns ErrUnexpectedEOF
							err = io.EOF
						}
					} else if size == 0 && err == io.EOF && objectInfo.Size > 0 {
						// Special cases when server writes more data
						// than the content-length, net/http response
						// body returns an error, instead of converting
						// it to io.EOF - return unexpected EOF.
						err = io.ErrUnexpectedEOF
					}

					// Reply back how much was read.
					resCh <- getResponse{
						Size:       size,
						Error:      err,
						didRead:    true,
						objectInfo: objectInfo,
					}
				}
			}
		}
	}()

	// Create a newObject through the information sent back by reqCh.
	return newObject(reqCh, resCh, doneCh), nil
}

// get request message container to communicate with internal
// go-routine.
type getRequest struct {
	Buffer            []byte
	Offset            int64 // readAt offset.
	DidOffsetChange   bool  // Tracks the offset changes for Seek requests.
	beenRead          bool  // Determines if this is the first time an object is being read.
	isReadAt          bool  // Determines if this request is a request to a specific range
	isReadOp          bool  // Determines if this request is a Read or Read/At request.
	isFirstReq        bool  // Determines if this request is the first time an object is being accessed.
	settingObjectInfo bool  // Determines if this request is to set the objectInfo of an object.
}

// get response message container to reply back for the request.
type getResponse struct {
	Size       int
	Error      error
	didRead    bool       // Lets subsequent calls know whether or not httpReader has been initiated.
	objectInfo ObjectInfo // Used for the first request.
}

// Object represents an open object. It implements
// Reader, ReaderAt, Seeker, Closer for a HTTP stream.
type Object struct {
	// Mutex.
	mutex *sync.Mutex

	// User allocated and defined.
	reqCh      chan<- getRequest
	resCh      <-chan getResponse
	doneCh     chan<- struct{}
	currOffset int64
	objectInfo ObjectInfo

	// Ask lower level to initiate data fetching based on currOffset
	seekData bool

	// Keeps track of closed call.
	isClosed bool

	// Keeps track of if this is the first call.
	isStarted bool

	// Previous error saved for future calls.
	prevErr error

	// Keeps track of if this object has been read yet.
	beenRead bool

	// Keeps track of if objectInfo has been set yet.
	objectInfoSet bool
}

// doGetRequest - sends and blocks on the firstReqCh and reqCh of an object.
// Returns back the size of the buffer read, if anything was read, as well
// as any error encountered. For all first requests sent on the object
// it is also responsible for sending back the objectInfo.
func (o *Object) doGetRequest(request getRequest) (getResponse, error) {
	o.reqCh <- request
	response := <-o.resCh

	// Return any error to the top level.
	if response.Error != nil {
		return response, response.Error
	}

	// This was the first request.
	if !o.isStarted {
		// The object has been operated on.
		o.isStarted = true
	}
	// Set the objectInfo if the request was not readAt
	// and it hasn't been set before.
	if !o.objectInfoSet && !request.isReadAt {
		o.objectInfo = response.objectInfo
		o.objectInfoSet = true
	}
	// Set beenRead only if it has not been set before.
	if !o.beenRead {
		o.beenRead = response.didRead
	}
	// Data are ready on the wire, no need to reinitiate connection in lower level
	o.seekData = false

	return response, nil
}

// setOffset - handles the setting of offsets for
// Read/ReadAt/Seek requests.
func (o *Object) setOffset(bytesRead int64) error {
	// Update the currentOffset.
	o.currOffset += bytesRead

	if o.objectInfo.Size > -1 && o.currOffset >= o.objectInfo.Size {
		return io.EOF
	}
	return nil
}

// Read reads up to len(b) bytes into b. It returns the number of
// bytes read (0 <= n <= len(b)) and any error encountered. Returns
// io.EOF upon end of file.
func (o *Object) Read(b []byte) (n int, err error) {
	if o == nil {
		return 0, errInvalidArgument("Object is nil")
	}

	// Locking.
	o.mutex.Lock()
	defer o.mutex.Unlock()

	// prevErr is previous error saved from previous operation.
	if o.prevErr != nil || o.isClosed {
		return 0, o.prevErr
	}

	// Create a new request.
	readReq := getRequest{
		isReadOp: true,
		beenRead: o.beenRead,
		Buffer:   b,
	}

	// Alert that this is the first request.
	if !o.isStarted {
		readReq.isFirstReq = true
	}

	// Ask to establish a new data fetch routine based on seekData flag
	readReq.DidOffsetChange = o.seekData
	readReq.Offset = o.currOffset

	// Send and receive from the first request.
	response, err := o.doGetRequest(readReq)
	if err != nil && err != io.EOF {
		// Save the error for future calls.
		o.prevErr = err
		return response.Size, err
	}

	// Bytes read.
	bytesRead := int64(response.Size)

	// Set the new offset.
	oerr := o.setOffset(bytesRead)
	if oerr != nil {
		// Save the error for future calls.
		o.prevErr = oerr
		return response.Size, oerr
	}

	// Return the response.
	return response.Size, err
}

// Stat returns the ObjectInfo structure describing Object.
func (o *Object) Stat() (ObjectInfo, error) {
	if o == nil {
		return ObjectInfo{}, errInvalidArgument("Object is nil")
	}
	// Locking.
	o.mutex.Lock()
	defer o.mutex.Unlock()

	if o.prevErr != nil && o.prevErr != io.EOF || o.isClosed {
		return ObjectInfo{}, o.prevErr
	}

	// This is the first request.
	if !o.isStarted || !o.objectInfoSet {
		// Send the request and get the response.
		_, err := o.doGetRequest(getRequest{
			isFirstReq:        !o.isStarted,
			settingObjectInfo: !o.objectInfoSet,
		})
		if err != nil {
			o.prevErr = err
			return ObjectInfo{}, err
		}
	}

	return o.objectInfo, nil
}

// ReadAt reads len(b) bytes from the File starting at byte offset
// off. It returns the number of bytes read and the error, if any.
// ReadAt always returns a non-nil error when n < len(b). At end of
// file, that error is io.EOF.
func (o *Object) ReadAt(b []byte, offset int64) (n int, err error) {
	if o == nil {
		return 0, errInvalidArgument("Object is nil")
	}

	// Locking.
	o.mutex.Lock()
	defer o.mutex.Unlock()

	// prevErr is error which was saved in previous operation.
	if o.prevErr != nil && o.prevErr != io.EOF || o.isClosed {
		return 0, o.prevErr
	}

	// Set the current offset to ReadAt offset, because the current offset will be shifted at the end of this method.
	o.currOffset = offset

	// Can only compare offsets to size when size has been set.
	if o.objectInfoSet {
		// If offset is negative than we return io.EOF.
		// If offset is greater than or equal to object size we return io.EOF.
		if (o.objectInfo.Size > -1 && offset >= o.objectInfo.Size) || offset < 0 {
			return 0, io.EOF
		}
	}

	// Create the new readAt request.
	readAtReq := getRequest{
		isReadOp:        true,
		isReadAt:        true,
		DidOffsetChange: true,       // Offset always changes.
		beenRead:        o.beenRead, // Set if this is the first request to try and read.
		Offset:          offset,     // Set the offset.
		Buffer:          b,
	}

	// Alert that this is the first request.
	if !o.isStarted {
		readAtReq.isFirstReq = true
	}

	// Send and receive from the first request.
	response, err := o.doGetRequest(readAtReq)
	if err != nil && err != io.EOF {
		// Save the error.
		o.prevErr = err
		return response.Size, err
	}
	// Bytes read.
	bytesRead := int64(response.Size)
	// There is no valid objectInfo yet
	// 	to compare against for EOF.
	if !o.objectInfoSet {
		// Update the currentOffset.
		o.currOffset += bytesRead
	} else {
		// If this was not the first request update
		// the offsets and compare against objectInfo
		// for EOF.
		oerr := o.setOffset(bytesRead)
		if oerr != nil {
			o.prevErr = oerr
			return response.Size, oerr
		}
	}
	return response.Size, err
}

// Seek sets the offset for the next Read or Write to offset,
// interpreted according to whence: 0 means relative to the
// origin of the file, 1 means relative to the current offset,
// and 2 means relative to the end.
// Seek returns the new offset and an error, if any.
//
// Seeking to a negative offset is an error. Seeking to any positive
// offset is legal, subsequent io operations succeed until the
// underlying object is not closed.
func (o *Object) Seek(offset int64, whence int) (n int64, err error) {
	if o == nil {
		return 0, errInvalidArgument("Object is nil")
	}

	// Locking.
	o.mutex.Lock()
	defer o.mutex.Unlock()

	// At EOF seeking is legal allow only io.EOF, for any other errors we return.
	if o.prevErr != nil && o.prevErr != io.EOF {
		return 0, o.prevErr
	}

	// Negative offset is valid for whence of '2'.
	if offset < 0 && whence != 2 {
		return 0, errInvalidArgument(fmt.Sprintf("Negative position not allowed for %d", whence))
	}

	// This is the first request. So before anything else
	// get the ObjectInfo.
	if !o.isStarted || !o.objectInfoSet {
		// Create the new Seek request.
		seekReq := getRequest{
			isReadOp:   false,
			Offset:     offset,
			isFirstReq: true,
		}
		// Send and receive from the seek request.
		_, err := o.doGetRequest(seekReq)
		if err != nil {
			// Save the error.
			o.prevErr = err
			return 0, err
		}
	}

	// Switch through whence.
	switch whence {
	default:
		return 0, errInvalidArgument(fmt.Sprintf("Invalid whence %d", whence))
	case 0:
		if o.objectInfo.Size > -1 && offset > o.objectInfo.Size {
			return 0, io.EOF
		}
		o.currOffset = offset
	case 1:
		if o.objectInfo.Size > -1 && o.currOffset+offset > o.objectInfo.Size {
			return 0, io.EOF
		}
		o.currOffset += offset
	case 2:
		// If we don't know the object size return an error for io.SeekEnd
		if o.objectInfo.Size < 0 {
			return 0, errInvalidArgument("Whence END is not supported when the object size is unknown")
		}
		// Seeking to positive offset is valid for whence '2', but
		// since we are backing a Reader we have reached 'EOF' if
		// offset is positive.
		if offset > 0 {
			return 0, io.EOF
		}
		// Seeking to negative position not allowed for whence.
		if o.objectInfo.Size+offset < 0 {
			return 0, errInvalidArgument(fmt.Sprintf("Seeking at negative offset not allowed for %d", whence))
		}
		o.currOffset = o.objectInfo.Size + offset
	}
	// Reset the saved error since we successfully seeked, let the Read
	// and ReadAt decide.
	if o.prevErr == io.EOF {
		o.prevErr = nil
	}

	// Ask lower level to fetch again from source
	o.seekData = true

	// Return the effective offset.
	return o.currOffset, nil
}

// Close - The behavior of Close after the first call returns error
// for subsequent Close() calls.
func (o *Object) Close() (err error) {
	if o == nil {
		return errInvalidArgument("Object is nil")
	}
	// Locking.
	o.mutex.Lock()
	defer o.mutex.Unlock()

	// if already closed return an error.
	if o.isClosed {
		return o.prevErr
	}

	// Close successfully.
	close(o.doneCh)

	// Save for future operations.
	errMsg := "Object is already closed. Bad file descriptor."
	o.prevErr = errors.New(errMsg)
	// Save here that we closed done channel successfully.
	o.isClosed = true
	return nil
}

// newObject instantiates a new *minio.Object*
// ObjectInfo will be set by setObjectInfo
func newObject(reqCh chan<- getRequest, resCh <-chan getResponse, doneCh chan<- struct{}) *Object {
	return &Object{
		mutex:  &sync.Mutex{},
		reqCh:  reqCh,
		resCh:  resCh,
		doneCh: doneCh,
	}
}

// getObject - retrieve object from Object Storage.
//
// Additionally this function also takes range arguments to download the specified
// range bytes of an object. Setting offset and length = 0 will download the full object.
//
// For more information about the HTTP Range header.
// go to http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35.
func (c *Client) getObject(ctx context.Context, bucketName, objectName string, opts GetObjectOptions) (io.ReadCloser, ObjectInfo, http.Header, error) {
	// Validate input arguments.
	if err := s3utils.CheckValidBucketName(bucketName); err != nil {
		return nil, ObjectInfo{}, nil, err
	}
	if err := s3utils.CheckValidObjectName(objectName); err != nil {
		return nil, ObjectInfo{}, nil, err
	}

	urlValues := make(url.Values)
	if opts.VersionID != "" {
		urlValues.Set("versionId", opts.VersionID)
	}
	if opts.PartNumber > 0 {
		urlValues.Set("partNumber", strconv.Itoa(opts.PartNumber))
	}

	// Execute GET on objectName.
	resp, err := c.executeMethod(ctx, http.MethodGet, requestMetadata{
		bucketName:       bucketName,
		objectName:       objectName,
		queryValues:      urlValues,
		customHeader:     opts.Header(),
		contentSHA256Hex: emptySHA256Hex,
	})
	if err != nil {
		return nil, ObjectInfo{}, nil, err
	}
	if resp != nil {
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			return nil, ObjectInfo{}, nil, httpRespToErrorResponse(resp, bucketName, objectName)
		}
	}

	objectStat, err := ToObjectInfo(bucketName, objectName, resp.Header)
	if err != nil {
		closeResponse(resp)
		return nil, ObjectInfo{}, nil, err
	}

	// do not close body here, caller will close
	return resp.Body, objectStat, resp.Header, nil
}
