package mapiter

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

// Iterate creates an iterator from arbitrary map types. This is not
// the most efficient tool, but it's the quickest way to create an
// iterator for maps.
// Also, note that you cannot make any assumptions on the order of
// pairs being returned.
func Iterate(ctx context.Context, m interface{}) (Iterator, error) {
	mrv := reflect.ValueOf(m)

	if mrv.Kind() != reflect.Map {
		return nil, fmt.Errorf(`argument must be a map (%s)`, mrv.Type())
	}

	ch := make(chan *Pair)
	go func(ctx context.Context, ch chan *Pair, mrv reflect.Value) {
		defer close(ch)
		for _, key := range mrv.MapKeys() {
			value := mrv.MapIndex(key)
			pair := &Pair{
				Key:   key.Interface(),
				Value: value.Interface(),
			}
			select {
			case <-ctx.Done():
				return
			case ch <- pair:
			}
		}
	}(ctx, ch, mrv)

	return New(ch), nil
}

// Source represents a map that knows how to create an iterator
type Source interface {
	Iterate(context.Context) Iterator
}

// Pair represents a single pair of key and value from a map
type Pair struct {
	Key   interface{}
	Value interface{}
}

// Iterator iterates through keys and values of a map
type Iterator interface {
	Next(context.Context) bool
	Pair() *Pair
}

type iter struct {
	ch   chan *Pair
	mu   sync.RWMutex
	next *Pair
}

// Visitor represents an object that handles each pair in a map
type Visitor interface {
	Visit(interface{}, interface{}) error
}

// VisitorFunc is a type of Visitor based on a function
type VisitorFunc func(interface{}, interface{}) error

func (fn VisitorFunc) Visit(s interface{}, v interface{}) error {
	return fn(s, v)
}

func New(ch chan *Pair) Iterator {
	return &iter{
		ch: ch,
	}
}

// Next returns true if there are more items to read from the iterator
func (i *iter) Next(ctx context.Context) bool {
	i.mu.RLock()
	if i.ch == nil {
		i.mu.RUnlock()
		return false
	}
	i.mu.RUnlock()

	i.mu.Lock()
	defer i.mu.Unlock()
	select {
	case <-ctx.Done():
		i.ch = nil
		return false
	case v, ok := <-i.ch:
		if !ok {
			i.ch = nil
			return false
		}
		i.next = v
		return true
	}

	//nolint:govet
	return false // never reached
}

// Pair returns the currently buffered Pair. Calling Next() will reset its value
func (i *iter) Pair() *Pair {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.next
}

// Walk walks through each element in the map
func Walk(ctx context.Context, s Source, v Visitor) error {
	for i := s.Iterate(ctx); i.Next(ctx); {
		pair := i.Pair()
		if err := v.Visit(pair.Key, pair.Value); err != nil {
			return fmt.Errorf(`failed to visit key %s: %w`, pair.Key, err)
		}
	}
	return nil
}

// AsMap returns the values obtained from the source as a map
func AsMap(ctx context.Context, s interface{}, v interface{}) error {
	var iter Iterator
	switch reflect.ValueOf(s).Kind() {
	case reflect.Map:
		x, err := Iterate(ctx, s)
		if err != nil {
			return fmt.Errorf(`failed to iterate over map type: %w`, err)
		}
		iter = x
	default:
		ssrc, ok := s.(Source)
		if !ok {
			return fmt.Errorf(`cannot iterate over %T: not a mapiter.Source type`, s)
		}
		iter = ssrc.Iterate(ctx)
	}

	dst := reflect.ValueOf(v)

	// dst MUST be a pointer to a map type
	if kind := dst.Kind(); kind != reflect.Ptr {
		return fmt.Errorf(`dst must be a pointer to a map (%s)`, dst.Type())
	}

	dst = dst.Elem()
	if dst.Kind() != reflect.Map {
		return fmt.Errorf(`dst must be a pointer to a map (%s)`, dst.Type())
	}

	if dst.IsNil() {
		dst.Set(reflect.MakeMap(dst.Type()))
	}

	// dst must be assignable
	if !dst.CanSet() {
		return fmt.Errorf(`dst is not writeable`)
	}

	keytyp := dst.Type().Key()
	valtyp := dst.Type().Elem()

	for iter.Next(ctx) {
		pair := iter.Pair()

		rvkey := reflect.ValueOf(pair.Key)
		rvvalue := reflect.ValueOf(pair.Value)

		if !rvkey.Type().AssignableTo(keytyp) {
			return fmt.Errorf(`cannot assign key of type %s to map key of type %s`, rvkey.Type(), keytyp)
		}

		switch rvvalue.Kind() {
		// we can only check if we can assign to rvvalue to valtyp if it's non-nil
		case reflect.Invalid:
			rvvalue = reflect.New(valtyp).Elem()
		default:
			if !rvvalue.Type().AssignableTo(valtyp) {
				return fmt.Errorf(`cannot assign value of type %s to map value of type %s`, rvvalue.Type(), valtyp)
			}
		}

		dst.SetMapIndex(rvkey, rvvalue)
	}

	return nil
}
