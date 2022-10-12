package jwk

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/lestrrat-go/iter/arrayiter"
	"github.com/lestrrat-go/jwx/internal/json"
	"github.com/lestrrat-go/jwx/internal/pool"
	"github.com/pkg/errors"
)

const keysKey = `keys` // appease linter

// NewSet creates and empty `jwk.Set` object
func NewSet() Set {
	return &set{
		privateParams: make(map[string]interface{}),
	}
}

func (s *set) Set(n string, v interface{}) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if n == keysKey {
		vl, ok := v.([]Key)
		if !ok {
			return errors.Errorf(`value for field "keys" must be []jwk.Key`)
		}
		s.keys = vl
		return nil
	}

	s.privateParams[n] = v
	return nil
}

func (s *set) Field(n string) (interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, ok := s.privateParams[n]
	return v, ok
}

func (s *set) Get(idx int) (Key, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if idx >= 0 && idx < len(s.keys) {
		return s.keys[idx], true
	}
	return nil, false
}

func (s *set) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.keys)
}

// indexNL is Index(), but without the locking
func (s *set) indexNL(key Key) int {
	for i, k := range s.keys {
		if k == key {
			return i
		}
	}
	return -1
}

func (s *set) Index(key Key) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.indexNL(key)
}

func (s *set) Add(key Key) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if i := s.indexNL(key); i > -1 {
		return false
	}
	s.keys = append(s.keys, key)
	return true
}

func (s *set) Remove(key Key) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, k := range s.keys {
		if k == key {
			switch i {
			case 0:
				s.keys = s.keys[1:]
			case len(s.keys) - 1:
				s.keys = s.keys[:i]
			default:
				s.keys = append(s.keys[:i], s.keys[i+1:]...)
			}
			return true
		}
	}
	return false
}

func (s *set) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.keys = nil
}

func (s *set) Iterate(ctx context.Context) KeyIterator {
	ch := make(chan *KeyPair, s.Len())
	go iterate(ctx, s.keys, ch)
	return arrayiter.New(ch)
}

func iterate(ctx context.Context, keys []Key, ch chan *KeyPair) {
	defer close(ch)

	for i, key := range keys {
		pair := &KeyPair{Index: i, Value: key}
		select {
		case <-ctx.Done():
			return
		case ch <- pair:
		}
	}
}

func (s *set) MarshalJSON() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	buf := pool.GetBytesBuffer()
	defer pool.ReleaseBytesBuffer(buf)
	enc := json.NewEncoder(buf)

	fields := []string{keysKey}
	for k := range s.privateParams {
		fields = append(fields, k)
	}
	sort.Strings(fields)

	buf.WriteByte('{')
	for i, field := range fields {
		if i > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(buf, `%q:`, field)
		if field != keysKey {
			if err := enc.Encode(s.privateParams[field]); err != nil {
				return nil, errors.Wrapf(err, `failed to marshal field %q`, field)
			}
		} else {
			buf.WriteByte('[')
			for j, k := range s.keys {
				if j > 0 {
					buf.WriteByte(',')
				}
				if err := enc.Encode(k); err != nil {
					return nil, errors.Wrapf(err, `failed to marshal key #%d`, i)
				}
			}
			buf.WriteByte(']')
		}
	}
	buf.WriteByte('}')

	ret := make([]byte, buf.Len())
	copy(ret, buf.Bytes())
	return ret, nil
}

func (s *set) UnmarshalJSON(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.privateParams = make(map[string]interface{})
	s.keys = nil

	var options []ParseOption
	var ignoreParseError bool
	if dc := s.dc; dc != nil {
		if localReg := dc.Registry(); localReg != nil {
			options = append(options, withLocalRegistry(localReg))
		}
		ignoreParseError = dc.IgnoreParseError()
	}

	var sawKeysField bool
	dec := json.NewDecoder(bytes.NewReader(data))
LOOP:
	for {
		tok, err := dec.Token()
		if err != nil {
			return errors.Wrap(err, `error reading token`)
		}

		switch tok := tok.(type) {
		case json.Delim:
			// Assuming we're doing everything correctly, we should ONLY
			// get either '{' or '}' here.
			if tok == '}' { // End of object
				break LOOP
			} else if tok != '{' {
				return errors.Errorf(`expected '{', but got '%c'`, tok)
			}
		case string:
			switch tok {
			case "keys":
				sawKeysField = true
				var list []json.RawMessage
				if err := dec.Decode(&list); err != nil {
					return errors.Wrap(err, `failed to decode "keys"`)
				}

				for i, keysrc := range list {
					key, err := ParseKey(keysrc, options...)
					if err != nil {
						if !ignoreParseError {
							return errors.Wrapf(err, `failed to decode key #%d in "keys"`, i)
						}
						continue
					}
					s.keys = append(s.keys, key)
				}
			default:
				var v interface{}
				if err := dec.Decode(&v); err != nil {
					return errors.Wrapf(err, `failed to decode value for key %q`, tok)
				}
				s.privateParams[tok] = v
			}
		}
	}

	// This is really silly, but we can only detect the
	// lack of the "keys" field after going through the
	// entire object once
	// Not checking for len(s.keys) == 0, because it could be
	// an empty key set
	if !sawKeysField {
		key, err := ParseKey(data, options...)
		if err != nil {
			return errors.Wrapf(err, `failed to parse sole key in key set`)
		}
		s.keys = append(s.keys, key)
	}
	return nil
}

func (s *set) LookupKeyID(kid string) (Key, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n := s.Len()
	for i := 0; i < n; i++ {
		key, ok := s.Get(i)
		if !ok {
			return nil, false
		}
		if key.KeyID() == kid {
			return key, true
		}
	}
	return nil, false
}

func (s *set) DecodeCtx() DecodeCtx {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dc
}

func (s *set) SetDecodeCtx(dc DecodeCtx) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dc = dc
}

func (s *set) Clone() (Set, error) {
	s2 := &set{}

	s.mu.RLock()
	defer s.mu.RUnlock()

	s2.keys = make([]Key, len(s.keys))

	for i := 0; i < len(s.keys); i++ {
		s2.keys[i] = s.keys[i]
	}
	return s2, nil
}
