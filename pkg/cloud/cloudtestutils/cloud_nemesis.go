// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloudtestutils

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// RunCloudNemesisTest writes random objects to the provided storage instance
// and validates they can be read back correctly. It is intended to be used in
// tandem with HTTP fault injection to ensure our retry logic can handle
// transient errors from the cloud storage providers without propagating errors
// or corrupting data.
//
// TODO(jeffswenson): use this to test GCS and Azure
func RunCloudNemesisTest(t *testing.T, storage cloud.ExternalStorage) {
	nemesis := &cloudNemesis{
		storage:          storage,
		writeConcurrency: 1,
		readConcurrency:  40,
		listConcurrency:  1,
	}

	// We create a context here because we don't want to support a caller supplied
	// cancelation signal.
	ctx := context.Background()
	if err := nemesis.run(ctx, 2*time.Minute); err != nil {
		t.Fatalf("%+v", err)
	}

	require.Greater(t, nemesis.writeSuccesses.Load(), int64(5), "not enough completed writes")
	require.Greater(t, nemesis.readSuccesses.Load(), int64(200), "not enough completed reads")
	require.Greater(t, nemesis.listSuccesses.Load(), int64(5), "not enough completed lists")
}

type cloudNemesis struct {
	storage          cloud.ExternalStorage
	writeConcurrency int
	readConcurrency  int
	listConcurrency  int

	readSuccesses  atomic.Int64
	writeSuccesses atomic.Int64
	listSuccesses  atomic.Int64

	mu struct {
		syncutil.Mutex
		objects []cloudObject
	}
}

type cloudObject struct {
	index int
	name  string

	size int

	finished bool
}

func (c *cloudNemesis) run(ctx context.Context, duration time.Duration) error {
	// NOTE: we don't use the context to cancel operations because we want to
	// ensure that in-flight operations running during fault injection eventually
	// run to completion.
	done := make(chan struct{})

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		time.Sleep(duration)
		close(done)
		return nil
	})

	for i := 0; i < c.writeConcurrency; i++ {
		g.Go(func() error {
			for {
				time.Sleep(time.Millisecond)
				select {
				case <-done:
					return nil
				default:
					if err := c.writeObject(ctx); err != nil {
						return err
					}
				}
			}
		})
	}

	for i := 0; i < c.readConcurrency; i++ {
		g.Go(func() error {
			for {
				time.Sleep(time.Millisecond)
				select {
				case <-done:
					return nil
				default:
					if err := c.readObject(ctx); err != nil {
						return err
					}
				}
			}
		})
	}

	for i := 0; i < c.listConcurrency; i++ {
		g.Go(func() error {
			for {
				time.Sleep(time.Millisecond)
				select {
				case <-done:
					return nil
				default:
					if err := c.listObjects(ctx); err != nil {
						return err
					}
				}
			}
		})
	}

	// We shouldn't observe any errors from the client. We are injecting errors
	// that should be transparently retried.
	return g.Wait()
}

func (c *cloudNemesis) writeObject(ctx context.Context) (err error) {
	// We test objects that are ~128 MiB in size because that is roughly the size
	// of a single SSTable in a backup.
	o := c.newObject()

	w, err := c.storage.Writer(ctx, o.name)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.CombineErrors(err, w.Close())
		if err == nil {
			c.finishObject(o)
			c.writeSuccesses.Add(1)
		}
	}()

	content := &generatedObject{
		seed:   int64(o.index),
		cursor: 0,
		size:   o.size,
	}

	_, err = io.Copy(w, content)
	if err != nil {
		return err
	}

	return nil
}

func (c *cloudNemesis) readObject(ctx context.Context) (err error) {
	obj := c.randomObject()
	if !obj.finished {
		// We only read finished objects
		return nil
	}

	// Generate a random read range that is at most ~4 KiB in size. We keep the
	// reads small so that we can execute them quickly and with high concurrency
	// to get more fault coverage. If we read the entire object we would risk
	// OOMs.
	start := rand.Intn(obj.size - 1)
	end := min(start+rand.Intn(4096)+1, obj.size)

	r, size, err := c.storage.ReadFile(ctx, obj.name, cloud.ReadOptions{
		Offset: int64(start),
	})
	if err != nil {
		return errors.Wrapf(err, "unable to read %s", obj.name)
	}
	defer func() {
		err = errors.CombineErrors(err, r.Close(ctx))
	}()

	if size != int64(obj.size) {
		return errors.AssertionFailedf("expected size %d, got %d", obj.size, size)
	}

	buf := make([]byte, end-start)
	readSize, err := io.ReadAtLeast(ioctx.ReaderCtxAdapter(ctx, r), buf[:], len(buf))
	if err != nil {
		return err
	}

	if readSize != len(buf) {
		return errors.AssertionFailedf("only read (%d/%d) bytes from %s", readSize, len(buf), obj.name)
	}

	expected := &generatedObject{
		seed:   int64(obj.index),
		cursor: start,
		size:   obj.size,
	}

	expectedBuf := make([]byte, end-start)
	_, err = expected.Read(expectedBuf)
	if err != nil {
		return errors.Wrap(err, "unable to read from expected generated object")
	}

	if !bytes.Equal(buf, expectedBuf) {
		return errors.AssertionFailedf("data mismatch for object %s [%d:%d]", obj.name, start, end)
	}

	c.readSuccesses.Add(1)

	return nil
}

func (c *cloudNemesis) listObjects(ctx context.Context) (err error) {
	before := c.snapshotObjects()

	listedFiles := map[string]bool{}
	err = c.storage.List(ctx, "", "", func(filename string) error {
		listedFiles[strings.TrimPrefix(filename, "/")] = true
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "unable to list files")
	}

	// Check if there are any missing files in the listing.
	for _, o := range before {
		if o.finished {
			if !listedFiles[o.name] {
				return errors.AssertionFailedf("expected to find object %s in listing", o.name)
			}
		}
	}

	// Check if there are any unexpected files in the listing.
	afterFiles := map[string]bool{}
	for _, o := range c.snapshotObjects() {
		afterFiles[o.name] = true
	}
	for filename := range listedFiles {
		if !afterFiles[filename] {
			return errors.AssertionFailedf("found unexpected object %s in listing", filename)
		}
	}

	c.listSuccesses.Add(1)
	return nil
}

func (c *cloudNemesis) newObject() cloudObject {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Objects are [0, 128 MiB) in size to match the size exported by backups.
	objectSize := max(1, rand.Intn(128*1024*1024))

	i := len(c.mu.objects)
	o := cloudObject{
		index:    i,
		name:     fmt.Sprintf("object-%d", i),
		size:     objectSize,
		finished: false,
	}

	c.mu.objects = append(c.mu.objects, o)
	return o
}

func (c *cloudNemesis) finishObject(o cloudObject) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.objects[o.index].finished = true
}

func (c *cloudNemesis) randomObject() cloudObject {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.mu.objects) == 0 {
		return cloudObject{}
	}

	return c.mu.objects[rand.Intn(len(c.mu.objects))]
}

func (c *cloudNemesis) snapshotObjects() []cloudObject {
	c.mu.Lock()
	defer c.mu.Unlock()

	snapshot := make([]cloudObject, len(c.mu.objects))
	copy(snapshot, c.mu.objects)
	return snapshot
}

// generatedObject is a deterministic implementation of io.Reader.
type generatedObject struct {
	seed   int64
	cursor int
	size   int
}

func (g *generatedObject) Read(p []byte) (n int, err error) {
	if g.cursor >= g.size {
		return 0, io.EOF
	}

	// Every byte is pos*seed % 256
	n = min(len(p), g.size-g.cursor)
	for i := 0; i < n; i++ {
		p[i] = byte((g.cursor + i) * int(g.seed) % 256)
	}
	g.cursor += n
	return n, nil
}
