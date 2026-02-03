package txnmode

import (
	"context"

	scheduler "github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnscheduler"
)

type Coordinator struct {
	feed OrderedFeed
	synthesizer LockSynthesizer
	scheduler scheduler.Scheduler
	writer TransactionWriter
}

func (c *Coordinator) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		writeSet, err := c.feed.Next(ctx)
		if err != nil {
			return err
		}

		err = c.writer.WriteTransaction(ctx, writeSet)
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}
