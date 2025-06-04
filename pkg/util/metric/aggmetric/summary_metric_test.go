package aggmetric

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

func TestSummaryMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r := metric.NewRegistry()
	writePrometheusMetrics := WritePrometheusMetricsFunc(r)

	counter := NewSummaryCounter(metric.Metadata{Name: "test.counter"}, "tenant_id", "storage_id")
	r.AddMetric(counter)

	counter.Inc(1, "1", "5")

	fmt.Println(writePrometheusMetrics(t))
}

func TestEviction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r := metric.NewRegistry()

	writePrometheusMetrics := WritePrometheusMetricsFunc(r)

	counter := NewSummaryCounter(metric.Metadata{Name: "test.counter"}, "tenant_id", "storage_id")
	cacheStorage := cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return size > 2
		},
	})
	counter.mu.children = &UnorderedCacheWrapper{
		cache: cacheStorage,
	}
	r.AddMetric(counter)

	counter2 := NewSummaryCounter(metric.Metadata{Name: "test.counter.2"}, "tenant_id", "storage_id")
	cacheStorage2 := cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return size > 2
		},
	})
	counter2.mu.children = &UnorderedCacheWrapper{
		cache: cacheStorage2,
	}
	r.AddMetric(counter2)

	counter.Inc(1, "1", "5")
	counter.Inc(1, "2", "5")

	counter2.Inc(1, "1", "5")
	counter2.Inc(1, "2", "5")

	fmt.Println(writePrometheusMetrics(t))

	counter.Inc(1, "3", "5")
	counter2.Inc(1, "3", "5")

	fmt.Println(writePrometheusMetrics(t))

}
