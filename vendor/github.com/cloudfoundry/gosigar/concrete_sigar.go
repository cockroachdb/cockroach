package sigar

import (
	"time"
)

type ConcreteSigar struct{}

func (c *ConcreteSigar) CollectCpuStats(collectionInterval time.Duration) (<-chan Cpu, chan<- struct{}) {
	// samplesCh is buffered to 1 value to immediately return first CPU sample
	samplesCh := make(chan Cpu, 1)

	stopCh := make(chan struct{})

	go func() {
		var cpuUsage Cpu

		// Immediately provide non-delta value.
		// samplesCh is buffered to 1 value, so it will not block.
		cpuUsage.Get()
		samplesCh <- cpuUsage

		ticker := time.NewTicker(collectionInterval)

		for {
			select {
			case <-ticker.C:
				previousCpuUsage := cpuUsage

				cpuUsage.Get()

				select {
				case samplesCh <- cpuUsage.Delta(previousCpuUsage):
				default:
					// Include default to avoid channel blocking
				}

			case <-stopCh:
				return
			}
		}
	}()

	return samplesCh, stopCh
}

func (c *ConcreteSigar) GetLoadAverage() (LoadAverage, error) {
	l := LoadAverage{}
	err := l.Get()
	return l, err
}

func (c *ConcreteSigar) GetMem() (Mem, error) {
	m := Mem{}
	err := m.Get()
	return m, err
}

func (c *ConcreteSigar) GetSwap() (Swap, error) {
	s := Swap{}
	err := s.Get()
	return s, err
}

func (c *ConcreteSigar) GetFileSystemUsage(path string) (FileSystemUsage, error) {
	f := FileSystemUsage{}
	err := f.Get(path)
	return f, err
}
