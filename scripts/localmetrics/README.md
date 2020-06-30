# Local timeseries tooling

## Quick Start

```
docker-compose up -d
./import-csv.sh < (curl https://gist.githubusercontent.com/tbg/98d9814f624629833e6cfb7d25cb8258/raw/70a96d50032361f864b240dbd9f1c36c385b7515/sample.csv)
# User/Pass admin/x
open http://127.0.0.1:3000
```

## Usage:

### Step 1: procure a metrics dump

The source of this could be a `debug zip` (pending [#50432]), or

`./cockroach debug tsdump --format=csv --host=... > dump.csv`.

### Step 2: `docker-compose up -d`

Not much more to be said. Unsurprisingly, this needs Docker to work. Omit the
`-d` if you want to see what's going on behind the scenes. It may take a moment
for the next step to work.

### Step 3: `./import-csv.sh < dump.csv`

This loads the CSV data into your local Postgres instance. Note that it will
truncate any existing data you have imported, so you're not accidentally mixing
metrics from various sources.

If you legitimately want to import multiple csvs at once, use `cat *.csv |
./import-csv` instead.

### Step 3: open [Grafana](http://127.0.0.1:3000)

Log in as user `admin`, password `x`. You should be able to find a reference to
the CockroachDB dashboard on the landing page.

### Step 4: Play

You can edit the provided panel or add panels to plot interesting metrics.
A good starting point for learning how to do that is the [Grafana blog].

Replace ./grafana/dashboards/home.json if you want the changes to persist.

TODO(tbg): auto-generate a better home.json from `pkg/ts/catalog`.

### Step 5: docker-compose stop

To avoid hogging resources on your machine. The postgres database is on your
local file system, so it will remain. If you want to nuke everything, use
`down` instead of `stop` and then `git clean -f .`.

[#50432]: https://github.com/cockroachdb/cockroach/pull/50432
[Grafana blog]: https://grafana.com/blog/2018/10/15/make-time-series-exploration-easier-with-the-postgresql/timescaledb-query-editor/
