package arrayiter

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

func Iterate(ctx context.Context, a interface{}) (Iterator, error) {
	arv := reflect.ValueOf(a)

	switch arv.Kind() {
	case reflect.Array, reflect.Slice:
	default:
		return nil, fmt.Errorf(`argument must be an array/slice (%s)`, arv.Type())
	}

	ch := make(chan *Pair)
	go func(ctx context.Context, ch chan *Pair, arv reflect.Value) {
		defer close(ch)

		for i := 0; i < arv.Len(); i++ {
			value := arv.Index(i)
			pair := &Pair{
				Index: i,
				Value: value.Interface(),
			}
			select {
			case <-ctx.Done():
				return
			case ch <- pair:
			}
		}
	}(ctx, ch, arv)

	return New(ch), nil
}

// Source represents a array that knows how to create an iterator
type Source interface {
	Iterate(context.Context) Iterator
}

// Pair represents a single pair of key and value from a array
type Pair struct {
	Index int
	Value interface{}
}

// Iterator iterates through keys and values of a array
type Iterator interface {
	Next(context.Context) bool
	Pair() *Pair
}

type iter struct {
	ch   chan *Pair
	mu   sync.RWMutex
	next *Pair
}

// Visitor represents an object that handles each pair in a array
type Visitor interface {
	Visit(int, interface{}) error
}

// VisitorFunc is a type of Visitor based on a function
type VisitorFunc func(int, interface{}) error

func (fn VisitorFunc) Visit(s int, v interface{}) error {
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

// Walk walks through each element in the array
func Walk(ctx context.Context, s Source, v Visitor) error {
	for i := s.Iterate(ctx); i.Next(ctx); {
		pair := i.Pair()
		if err := v.Visit(pair.Index, pair.Value); err != nil {
			return fmt.Errorf(`failed to visit index %d: %w`, pair.Index, err)
		}
	}
	return nil
}

func AsArray(ctx context.Context, s interface{}, v interface{}) error {
	var iter Iterator
	switch reflect.ValueOf(s).Kind() {
	case reflect.Array, reflect.Slice:
		x, err := Iterate(ctx, s)
		if err != nil {
			return fmt.Errorf(`failed to iterate over array/slice type: %w`, err)
		}
		iter = x
	default:
		ssrc, ok := s.(Source)
		if !ok {
			return fmt.Errorf(`cannot iterate over %T: not a arrayiter.Source type`, s)
		}
		iter = ssrc.Iterate(ctx)
	}

	dst := reflect.ValueOf(v)

	// dst MUST be a pointer to a array type
	if kind := dst.Kind(); kind != reflect.Ptr {
		return fmt.Errorf(`dst must be a pointer to a array (%s)`, dst.Type())
	}

	dst = dst.Elem()
	switch dst.Kind() {
	case reflect.Array, reflect.Slice:
	default:
		return fmt.Errorf(`dst must be a pointer to an array or slice (%s)`, dst.Type())
	}

	var pairs []*Pair
	for iter.Next(ctx) {
		pair := iter.Pair()
		pairs = append(pairs, pair)
	}

	switch dst.Kind() {
	case reflect.Array:
		if len(pairs) < dst.Len() {
			return fmt.Errorf(`dst array does not have enough space for elements (%d, want %d)`, dst.Len(), len(pairs))
		}
	case reflect.Slice:
		if dst.IsNil() {
			dst.Set(reflect.MakeSlice(dst.Type(), len(pairs), len(pairs)))
		}
	}

	// dst must be assignable
	if !dst.CanSet() {
		return fmt.Errorf(`dst is not writeable`)
	}

	elemtyp := dst.Type().Elem()
	for _, pair := range pairs {
		rvvalue := reflect.ValueOf(pair.Value)

		if !rvvalue.Type().AssignableTo(elemtyp) {
			return fmt.Errorf(`cannot assign key of type %s to map key of type %s`, rvvalue.Type(), elemtyp)
		}

		dst.Index(pair.Index).Set(rvvalue)
	}

	return nil
}
