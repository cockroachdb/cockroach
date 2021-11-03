package cdctest

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"gocloud.dev/pubsub"
)

const testMemPubsubURI = "mem://testfeedURL"

// MockPubsubSink is the Webhook sink used in tests.
type MockPubsubSink struct {
	sub *pubsub.Subscription
	ctx context.Context
	errChan chan error
	url string
	shutdown func()
	mu                 struct {
		syncutil.Mutex
		rows             []string
	}
}

func MakeMockPubsubSink(url string, ctx context.Context) (*MockPubsubSink, error){
	ctx, shutdown := context.WithCancel(ctx)
	p := &MockPubsubSink{ctx: ctx, errChan: make(chan error), url: url, shutdown: shutdown}
	return p, nil
}

func (p *MockPubsubSink) Close() {
	if p.sub != nil {
		p.sub.Shutdown(p.ctx)
	}
	p.shutdown()
	close(p.errChan)
}

func (p *MockPubsubSink) Dial() error{
	sub ,err := pubsub.OpenSubscription(p.ctx, p.url)
	if err != nil {
		return err
	}
	p.sub = sub
	go p.recieve()
	return nil
}

func (p *MockPubsubSink) recieve() {
	for {
		msg, err := p.sub.Receive(p.ctx)
		if err != nil {
			p.errChan <- err
		}
		msgBody := string(msg.Body)
		select {
		case <-p.ctx.Done():
			return
		default:
			if msgBody != "" {
				p.push(msgBody)
			}
		}
	}
}

func (p *MockPubsubSink) push(msg string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.rows = append(p.mu.rows, msg)
}

func (p *MockPubsubSink) Pop() string{
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.mu.rows) > 0 {
		oldest := p.mu.rows[0]
		p.mu.rows = p.mu.rows[1:]
		return oldest
	}
	return ""
}

func (p *MockPubsubSink)CheckSinkError() error{
	select {
		case err := <-p.errChan:
			return err
		default:
	}
	return nil
}







