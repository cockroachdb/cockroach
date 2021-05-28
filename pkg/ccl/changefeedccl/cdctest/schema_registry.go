// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdctest

import (
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/linkedin/goavro/v2"
)

// SchemaRegistry is the kafka schema registry used in tests.
type SchemaRegistry struct {
	server *httptest.Server
	mu     struct {
		syncutil.Mutex
		idAlloc  int32
		schemas  map[int32]string
		subjects map[string]int32
	}
}

// StartTestSchemaRegistry creates and starts schema registry for
// tests.
func StartTestSchemaRegistry() *SchemaRegistry {
	r := makeTestSchemaRegistry()
	r.server.Start()
	return r
}

// StartTestSchemaRegistryWithTLS creates and starts schema registry
// for tests with TLS enabled.
func StartTestSchemaRegistryWithTLS(certificate *tls.Certificate) (*SchemaRegistry, error) {
	r := makeTestSchemaRegistry()
	if certificate != nil {
		r.server.TLS = &tls.Config{
			Certificates: []tls.Certificate{*certificate},
		}
	}
	r.server.StartTLS()
	return r, nil
}

func makeTestSchemaRegistry() *SchemaRegistry {
	r := &SchemaRegistry{}
	r.mu.schemas = make(map[int32]string)
	r.mu.subjects = make(map[string]int32)
	r.server = httptest.NewUnstartedServer(http.HandlerFunc(r.requestHandler))
	return r
}

// Close closes this schema registry.
func (r *SchemaRegistry) Close() {
	r.server.Close()
}

// URL returns the http address of this schema registry.
func (r *SchemaRegistry) URL() string {
	return r.server.URL
}

// Subjects returns a copy of currently registered subjects.
func (r *SchemaRegistry) Subjects() (subjects []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for subject := range r.mu.subjects {
		subjects = append(subjects, subject)
	}
	return
}

// SchemaForSubject returns schema name for the specified subject.
func (r *SchemaRegistry) SchemaForSubject(subject string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.schemas[r.mu.subjects[subject]]
}

func (r *SchemaRegistry) registerSchema(subject string, schema string) int32 {
	r.mu.Lock()
	defer r.mu.Unlock()

	id := r.mu.idAlloc
	r.mu.idAlloc++
	r.mu.schemas[id] = schema
	r.mu.subjects[subject] = id
	return id
}

var (
	// We are slightly stricter than confluent here as they allow
	// a trailing slash.
	subjectVersionsRegexp = regexp.MustCompile("^/subjects/[^/]+/versions$")
)

// requestHandler routes requests based on the Method and Path of the request.
func (r *SchemaRegistry) requestHandler(hw http.ResponseWriter, hr *http.Request) {
	path := hr.URL.Path
	method := hr.Method

	var err error
	switch {
	case method == http.MethodPost && subjectVersionsRegexp.MatchString(path):
		err = r.register(hw, hr)
	case method == http.MethodGet && path == "/mode":
		err = r.mode(hw, hr)
	default:
		hw.WriteHeader(http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(hw, err.Error(), http.StatusInternalServerError)
	}
}

// register is an http handler for the underlying server which registers schemas.
func (r *SchemaRegistry) register(hw http.ResponseWriter, hr *http.Request) (err error) {
	type confluentSchemaVersionRequest struct {
		Schema string `json:"schema"`
	}
	type confluentSchemaVersionResponse struct {
		ID int32 `json:"id"`
	}

	defer func() {
		err = hr.Body.Close()
	}()

	var req confluentSchemaVersionRequest
	if err := json.NewDecoder(hr.Body).Decode(&req); err != nil {
		return err
	}

	subject := strings.Split(hr.URL.Path, "/")[2]
	id := r.registerSchema(subject, req.Schema)
	res, err := json.Marshal(confluentSchemaVersionResponse{ID: id})
	if err != nil {
		return err
	}

	hw.Header().Set(`Content-type`, `application/json`)
	_, err = hw.Write(res)
	return err
}

// mode is an http handler for the /mode endpoint. Our implementation
// returns an empty response as we currently don't care about the
// response.
func (r *SchemaRegistry) mode(hw http.ResponseWriter, _ *http.Request) error {
	_, err := hw.Write([]byte("{}"))
	return err
}

// EncodedAvroToNative decodes bytes that were previously encoded by
// confluent avro encoder, into GO native representation.
func (r *SchemaRegistry) EncodedAvroToNative(b []byte) (interface{}, error) {
	if len(b) == 0 || b[0] != changefeedbase.ConfluentAvroWireFormatMagic {
		return ``, errors.Errorf(`bad magic byte`)
	}
	b = b[1:]
	if len(b) < 4 {
		return ``, errors.Errorf(`missing registry id`)
	}
	id := int32(binary.BigEndian.Uint32(b[:4]))
	b = b[4:]

	r.mu.Lock()
	jsonSchema := r.mu.schemas[id]
	r.mu.Unlock()
	codec, err := goavro.NewCodec(jsonSchema)
	if err != nil {
		return ``, err
	}
	native, _, err := codec.NativeFromBinary(b)
	return native, err
}

// AvroToJSON converts avro bytes to their JSON representation.
func (r *SchemaRegistry) AvroToJSON(avroBytes []byte) ([]byte, error) {
	if len(avroBytes) == 0 {
		return nil, nil
	}
	native, err := r.EncodedAvroToNative(avroBytes)
	if err != nil {
		return nil, err
	}
	// The avro textual format is a more natural fit, but it's non-deterministic
	// because of go's randomized map ordering. Instead, we use json.Marshal,
	// which sorts its object keys and so is deterministic.
	return json.Marshal(native)
}
