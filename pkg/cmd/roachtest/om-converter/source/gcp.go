package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/model"
	"google.golang.org/api/iterator"
)

// Constants for configuration
const (
	// DefaultBufferSize is the size of the buffer used for reading files
	DefaultBufferSize = 10 * 1024 * 1024 // 10MB

	// MaxFileSize is the maximum size of a file to process
	MaxFileSize = 100 * 1024 * 1024 // 100MB

	// PollInterval is the duration to wait between checking for new objects
	PollInterval = time.Second

	// ChannelTimeout is the maximum time to wait when sending to channel
	ChannelTimeout = 5 * time.Second
)

// gcpSource implements the Source interface for Google Cloud Storage
type gcpSource struct {
	ctx    context.Context
	path   string
	client *storage.Client
	bucket *storage.BucketHandle
}

// NewGCPSource creates a new GCP storage source with the specified bucket and path
func NewGCPSource(ctx context.Context, bucket, path string) (Source, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}

	return &gcpSource{
		ctx:    ctx,
		bucket: client.Bucket(bucket),
		client: client,
		path:   path,
	}, nil
}

// Close implements the Source interface, cleaning up GCP client resources
func (g gcpSource) Close() {
	if err := g.client.Close(); err != nil {
		fmt.Printf("error closing gcp client: %v\n", err)
	}
}

// Start begins processing files from the GCP bucket, sending file info through the channel
func (g gcpSource) Start(ch chan model.FileInfo) error {
	processor := &gcpFileProcessor{
		source: g,
		ch:     ch,
	}
	return processor.process()
}

// gcpFileProcessor handles the file processing logic for GCP source
type gcpFileProcessor struct {
	source        gcpSource
	ch            chan model.FileInfo
	lastProcessed string
	currentReader *storage.Reader
}

// process continuously processes files from the GCP bucket
func (p *gcpFileProcessor) process() (err error) {
	defer p.cleanup()

	for {
		select {
		case <-p.source.ctx.Done():
			return context.Canceled
		default:
			if err := p.processNextBatch(); err != nil {
				return err
			}
			time.Sleep(PollInterval)
		}
	}
}

// processNextBatch processes the next batch of files from the bucket
func (p *gcpFileProcessor) processNextBatch() error {
	query := &storage.Query{
		Prefix:      p.source.path,
		StartOffset: p.lastProcessed,
	}

	it := p.source.bucket.Objects(p.source.ctx, query)

	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("error iterating through objects: %w", err)
		}

		if attrs.Name == p.lastProcessed {
			continue
		}

		// Skip files that are too large
		if attrs.Size > MaxFileSize {
			fmt.Printf("Skipping large file %s: size %d bytes exceeds limit of %d bytes\n",
				attrs.Name, attrs.Size, MaxFileSize)
			p.lastProcessed = attrs.Name
			continue
		}

		if err := p.processFile(attrs); err != nil {
			return err
		}

		p.lastProcessed = attrs.Name
	}

	return nil
}

// processFile handles reading and sending a single file's content
func (p *gcpFileProcessor) processFile(attrs *storage.ObjectAttrs) error {
	content, err := p.readFileContent(attrs.Name)
	if err != nil {
		return err
	}

	// Try to send with timeout to prevent blocking
	select {
	case p.ch <- model.FileInfo{
		Path:            attrs.Name,
		Content:         content,
		DirectoryPrefix: p.source.path,
	}:
		// Successfully sent
	case <-time.After(ChannelTimeout):
		return fmt.Errorf("timeout sending file %s to channel", attrs.Name)
	case <-p.source.ctx.Done():
		return context.Canceled
	}

	return nil
}

// readFileContent reads the entire content of a file from GCP storage
func (p *gcpFileProcessor) readFileContent(objectName string) ([]byte, error) {
	var err error
	p.currentReader, err = p.source.bucket.Object(objectName).NewReader(p.source.ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating reader for object %s: %w", objectName, err)
	}
	defer p.closeReader()

	return p.readAllContent(objectName)
}

// readAllContent reads all content from the current reader
func (p *gcpFileProcessor) readAllContent(objectName string) ([]byte, error) {
	// Pre-allocate buffer with known size
	content := make([]byte, 0, p.currentReader.Size())
	buf := make([]byte, DefaultBufferSize)

	for {
		n, err := p.currentReader.Read(buf)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("error reading content of object %s: %w", objectName, err)
		}
		if n == 0 {
			break
		}
		content = append(content, buf[:n]...)

		// Check if we've exceeded the maximum file size
		if len(content) > MaxFileSize {
			return nil, fmt.Errorf("file %s exceeds maximum size of %d bytes", objectName, MaxFileSize)
		}
	}

	return content, nil
}

// closeReader safely closes the current reader if it exists
func (p *gcpFileProcessor) closeReader() {
	if p.currentReader != nil {
		if err := p.currentReader.Close(); err != nil {
			fmt.Printf("error closing reader: %v\n", err)
		}
		p.currentReader = nil
	}
}

// cleanup performs final cleanup of resources
func (p *gcpFileProcessor) cleanup() {
	p.closeReader()
}
