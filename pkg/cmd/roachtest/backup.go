// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"
	"fmt"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/workload"
	_ "github.com/cockroachdb/cockroach/pkg/workload/bank"
)

func init() {
	// TODO(dan): Make this backup2TB once the fixture is ready.
	tests.Add(`backupTiny`, func(t *test) {
		const nodes = 1
		ctx := context.Background()
		c := newCluster(ctx, t, nodes)
		defer c.Destroy(ctx)

		meta, err := workload.Get(`bank`)
		if err != nil {
			t.Fatal(err)
		}
		gen := meta.New()
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			`--rows=100`, `--payload-bytes=1024`, `--ranges=0`,
		}); err != nil {
			t.Fatal(err)
		}
		c.RestoreStoreDirSnapshots(ctx, gen)

		c.Put(ctx, cockroachPath, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workloadPath, "./workload", c.Node(1))
		c.Start(ctx, c.Range(1, nodes))

		m := newMonitor(ctx, c, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			backupPath := (&url.URL{
				Scheme: gcsURLScheme,
				Host:   gcsTestBucket,
				Path:   c.name,
			}).String()
			cmd := fmt.Sprintf(`./cockroach sql --insecure -e "BACKUP workload.bank TO '%s'"`,
				backupPath)
			c.Run(ctx, 1, cmd)
			return nil
		})
		m.Wait()
	})
}
