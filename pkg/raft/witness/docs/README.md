# raft/witness — documentation

- [`site/`](./site/) — SvelteKit-based docs site (`cd site && npm run
  dev`). Page sources live in `site/src/routes/(docs)/` as `.svx`
  (markdown + Svelte) files. Includes:
  - **Introduction** — motivation and headline cost numbers.
  - **Witness as a Service** — proposed API for hosting witnesses
    outside the data plane.
  - **Cost details** — leader CPU and network costs of raft
    replication, with reproduction recipe.
