# Metrics (a.k.a. Transient Stats)

The files in this directory allow CockroachDB to track server metrics, which are persisted
to the time-series database and are viewable through the web interface and the
`/_status/metrics/<NODEID>` HTTP endpoint.

## Adding a new metric

1. Add the metric to `server/Server.registry`, which is passed to various places such as the
   `Server`'s instance of `sql.Executor`. Call methods such as `Counter()` and `Rate()` on the
   `Registry` to register the metric. For example, let's take this example from
   `sql.NewExecutor()`:

   ```
   exec := &Executor{
     ...
     selectCount: registry.Counter("sql.select.count"),
     ...
   }
   ```

   This code block registers the metric `sql.select.count`. The metric can then be accessed
   through the `selectCount` variable.
2. Update the metric in the appropriate places. Let's increment the counter introduced above:

   ```
   func (e *Executor) doSelect() {
     // do the SELECT
     e.selectCount.Inc(1)
   }
   ```
3. Add the metric to the web UI, which is in `ui/ts/pages/*.ts`. Someone more qualified than
   me can elaborate, like @maxlang.
4. Add tests! After your test does something to trigger your new metric update, you'll probably
   want to call `func (ts *TestServer) GetMetaRegistryMap()` and verify that the metric was
   updated correctly. See `sql/metric_test.go` for an example.

You can also manually verify that your metric is updating by using the metrics endpoint. For
example:

```
curl http://localhost:26257/_status/metrics/1

(some other output)
"cr.node.sql.select.count.1": 5,
(some other output)
```

Note that a prefix and suffix have been added. The prefix `cr.node.` denotes that this metric
is node-level. The suffix `.1` specifies that this metric is for node #1.
