package swissmap

import (
	"fmt"
	"math/bits"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasic(t *testing.T) {
	m := New[int, int](0)
	for i := 0; i < 10; i++ {
		m.Put(i, i+10)
		if v, ok := m.Get(i); ok {
			fmt.Printf("%d\n", v)
		}
	}
	for i := 0; i < 10; i++ {
		m.Delete(i)
		if v, ok := m.Get(i); ok {
			fmt.Printf("%d\n", v)
		}
	}
}

func BenchmarkStringMaps(b *testing.B) {
	const keySz = 8
	sizes := []int{16, 128, 1024, 8192, 131072}
	for _, n := range sizes {
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			b.Run("runtime map", func(b *testing.B) {
				benchmarkRuntimeMap(b, genStringData(keySz, n))
			})
			b.Run("swissmap", func(b *testing.B) {
				benchmarkSwissMap(b, genStringData(keySz, n))
			})
		})
	}
}

func BenchmarkInt64Maps(b *testing.B) {
	sizes := []int{16, 128, 1024, 8192, 131072}
	for _, n := range sizes {
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			b.Run("runtime map", func(b *testing.B) {
				benchmarkRuntimeMap(b, genInt64Data(n))
			})
			b.Run("swissmap", func(b *testing.B) {
				benchmarkSwissMap(b, genInt64Data(n))
			})
		})
	}
}

func benchmarkRuntimeMap[K comparable](b *testing.B, keys []K) {
	n := uint32(len(keys))
	mod := n - 1 // power of 2 fast modulus
	require.Equal(b, 1, bits.OnesCount32(n))
	m := make(map[K]K, n)
	for _, k := range keys {
		m[k] = k
	}
	b.ResetTimer()
	var ok bool
	for i := 0; i < b.N; i++ {
		_, ok = m[keys[uint32(i)&mod]]
	}
	assert.True(b, ok)
	// b.ReportAllocs()
}

func benchmarkSwissMap[K comparable](b *testing.B, keys []K) {
	n := uint32(len(keys))
	mod := n - 1 // power of 2 fast modulus
	require.Equal(b, 1, bits.OnesCount32(n))
	m := New[K, K](len(keys))
	for _, k := range keys {
		m.Put(k, k)
	}

	b.ResetTimer()
	var ok bool
	for i := 0; i < b.N; i++ {
		_, ok = m.Get(keys[uint32(i)&mod])
	}
	b.StopTimer()

	assert.True(b, ok)
	b.ReportMetric(float64(m.Len())/float64(m.capacity), "load-factor")
	// b.ReportAllocs()
}

func genStringData(size, count int) (keys []string) {
	src := rand.New(rand.NewSource(int64(size * count)))
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	r := make([]rune, size*count)
	for i := range r {
		r[i] = letters[src.Intn(len(letters))]
	}
	keys = make([]string, count)
	for i := range keys {
		keys[i] = string(r[:size])
		r = r[size:]
	}
	return
}

func genInt64Data(n int) (data []int64) {
	data = make([]int64, n)
	var x int64
	for i := range data {
		x += rand.Int63n(128) + 1
		data[i] = x
	}
	return
}
