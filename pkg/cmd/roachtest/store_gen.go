package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
)

var stores = 3

// registerStoreGen registers a store generation "test" that powers the
// 'roachtest store-gen' subcommand.
func registerStoreGen(r *registry, args []string) {
	workloadArgs := strings.Join(args, " ")

	r.Add(testSpec{
		Name:  "store-gen",
		Nodes: nodes(stores),
		Run: func(ctx context.Context, t *test, c *cluster) {
			c.Put(ctx, cockroach, "./cockroach")
			c.Put(ctx, workload, "./workload")
			c.Start(ctx)

			m := newMonitor(ctx, c)
			m.Go(func(ctx context.Context) error {
				t.Status("restoring fixture")
				c.Run(ctx, c.Node(1), fmt.Sprintf("./workload fixtures load %s", workloadArgs))

				out, err := c.RunWithBuffer(ctx, c.l, c.Node(1),
					fmt.Sprintf(`./workload fixtures url %s`, workloadArgs))
				if err != nil {
					t.Fatal(err)
				}
				fixturesURL := fmt.Sprintf("%s/stores=%d", bytes.TrimSpace(out), stores)

				t.Status("deleting old store dumps")
				c.Run(ctx, c.Node(1), fmt.Sprintf("gsutil -m -q rm -r %s", fixturesURL))

				t.Status("uploading store dumps")
				var wg sync.WaitGroup
				for store := 1; store <= stores; store++ {
					wg.Add(1)
					go func(store int) {
						defer wg.Done()
						c.Run(ctx, c.Node(store), fmt.Sprintf("gsutil -m -q cp -r {store-dir}/* %s/%d/",
							fixturesURL, store))
					}(store)
				}
				wg.Wait()
				return nil
			})
			m.Wait()
		},
	})
}
