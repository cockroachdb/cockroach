# DB Console Architecture

## Workspace Structure

DB Console is organized as a **pnpm monorepo** under `pkg/ui/workspaces/`:

- **`cluster-ui`** — Shared component library published as `@cockroachlabs/cluster-ui`. Contains API fetchers, SWR hooks, page-level components, and shared utilities. Used by both DB Console and CockroachCloud.
- **`db-console`** — The application shell. Provides routing, authentication, layout, and thin wrapper components that bridge any remaining app-level state to cluster-ui components.
- **`crdb-api-client`** — Protobuf client wrapper.
- **`e2e-tests`** — End-to-end test suite.
- **`eslint-plugin-crdb`** — Custom ESLint rules.

### How cluster-ui is consumed

db-console depends on cluster-ui via a local link (`"@cockroachlabs/cluster-ui": "link:../cluster-ui"`). Components, hooks, and utilities are imported directly:

```tsx
import { ScheduleDetails, useNodes } from "@cockroachlabs/cluster-ui";
```

---

## Data Fetching

Data fetching uses [SWR](https://swr.vercel.app/) (stale-while-revalidate). SWR handles caching, request deduplication, automatic revalidation, and error/loading states. Hooks and their fetcher functions are co-located in `cluster-ui/src/api/`.

### How it works

```
Component calls useMyData() hook
  -> hook calls useSwrWithClusterId(key, fetcher, options)
  -> SWR handles caching, deduplication, revalidation
  -> component reads { data, error, isLoading } directly
```

The hook itself is the store that is managed by the SWR library. This is different than our redux saga implementation, where we were responsible for managing these all these states

### Base HTTP utilities

Two fetcher utilities in `cluster-ui/src/api/fetchData.ts` handle the actual HTTP calls:

- **`fetchData(respBuilder, path, reqBuilder?, reqPayload?, timeout?)`** — For protobuf APIs (`_status/*`, `_admin/*`). Encodes requests and decodes responses using protobuf, sets `Grpc-Timeout` header (default 30s).
- **`fetchDataJSON<ResponseType, RequestType>(path, reqPayload?)`** — For JSON APIs (`/api/v2`). Standard `application/json` content type.

Both throw `RequestError` on non-2xx responses with extracted error messages.

### SWR hook variants

All three live in `cluster-ui/src/util/hooks.ts` and automatically prepend the cluster ID (from `ClusterDetailsContext`) to the SWR cache key. This ensures cache isolation across clusters.

| Hook | When to use |
|------|------------|
| `useSwrWithClusterId(key, fetcher, config?)` | Standard reads. Supports auto-revalidation, polling, deduplication. |
| `useSwrImmutableWithClusterId(key, fetcher, config?)` | Data that should be fetched once and never automatically revalidated (e.g. grants on a detail page). If another hook mutates or revalidates the same cache key, the updated value is still reflected. |
| `useSwrMutationWithClusterId(key, fetcher, config?)` | Write operations (POST, PUT, DELETE). Returns `{ trigger }` — call `trigger()` to execute the mutation imperatively. |

### ClusterDetailsContext

```tsx
type ClusterDetailsContextType = {
  isTenant?: boolean;   // true when running in tenant (serverless) mode
  clusterId?: string;   // unique cluster identifier for cache key scoping
};
```

Provided at the app root. Hooks use `isTenant` to conditionally disable fetchers for endpoints unavailable in tenant mode (pass `null` as the fetcher to skip).

### Writing a hook

Hooks live alongside their fetcher functions in `cluster-ui/src/api/<feature>Api.ts`.

```tsx
// cluster-ui/src/api/nodesApi.ts

// 1. Export the SWR key if other hooks need to share the cache.
export const NODES_SWR_KEY = "nodesUI";

// 2. Define the fetcher (plain async function).
export const getNodes = (): Promise<NodesResponse> => {
  return fetchData(NodesResponse, "_status/nodes_ui");
};

// 3. Define the hook.
export const useNodes = (opts?: { refreshInterval?: number }) => {
  const { isTenant } = useContext(ClusterDetailsContext);
  const { data, isLoading, error } = useSwrWithClusterId(
    NODES_SWR_KEY,
    !isTenant ? getNodes : null,  // null fetcher = skip for tenants
    {
      revalidateOnFocus: false,
      dedupingInterval: 10_000,
      refreshInterval: opts?.refreshInterval,  // caller opts in to polling
    },
  );

  return { nodes: data?.nodes ?? [], isLoading, error };
};
```

### SWR key design

- **Simple string** for singleton resources: `"nodesUI"`
- **Object** for parameterized resources: `{ name: "schedule", id: idStr }`
- **`null` key** to skip fetching: `shouldFetch ? { name: "logs", nodeId } : null`
- SWR supports arrays/objects natively — `JSON.stringify` is unnecessary.
- Export the key constant if other hooks need to share or invalidate the cache.

### SWR configuration

| Option | When to use |
|--------|------------|
| `refreshInterval` | Data that should poll (dashboards, live lists). Accept as an option so callers can opt in. |
| `revalidateOnFocus: false` | Expensive or rarely-changing data. |
| `dedupingInterval` | How long to dedup identical requests (default 2s). |

### Composing hooks

When a component needs data from multiple sources, compose hooks. SWR automatically deduplicates — if `useNodes()` is called from both `useNodesSummary()` and another component, only one API call is made.

```tsx
export const useNodesSummary = () => {
  const { nodeStatuses, isLoading: nodesLoading, error: nodesError } = useNodes();
  const { livenesses, isLoading: livenessLoading, error: livenessError } = useLiveness();

  const isLoading = nodesLoading || livenessLoading;

  const summary = useMemo(() => {
    // derive combined data — don't include isLoading in deps
  }, [nodeStatuses, livenesses]);

  return { ...summary, isLoading, error: nodesError ?? livenessError };
};
```

### Cache invalidation after mutations

Use the bound `mutate` from the read hook combined with `useSwrMutationWithClusterId` for writes:

```tsx
// Read hook — destructure and alias its mutate
const { data, mutate: refreshFiles } = useSwrWithClusterId(
  { name: "executionFiles", jobID },
  () => listExecutionDetailFiles({ job_id: jobID }),
);

// Write hook — refresh the read cache after success
const { trigger } = useSwrMutationWithClusterId(
  { name: "collectDetails", jobID },
  async () => {
    const resp = await collectExecutionDetails({ job_id: jobID });
    if (resp.req_resp) refreshFiles();
  },
);
```

Prefer the bound `mutate` from the hook that owns the cache key over SWR's global `mutate`.

---

## Component Patterns

### Functional components with hooks

All new components should be functional components using React hooks. All new components call SWR hooks directly for data:

```tsx
export const SchedulesPage: React.FC = () => {
  const { data, error, isLoading } = useSwrWithClusterId(
    { name: "schedules", status, limit },
    () => getSchedules({ status, limit }),
  );

  return (
    <Loading loading={isLoading} error={error} page="schedules">
      {/* render table with data */}
    </Loading>
  );
};
```

### What remains

A small number of components (approximately 13) still use the `connect()` pattern. When modifying these files, prefer converting to hooks (`useSelector`, `useDispatch`, and SWR hooks) rather than extending the legacy pattern. The data fetching should move to an SWR hook in `cluster-ui/src/api/`, and the db-console wrapper should become a thin functional component.

---

## Testing

The UI workspaces use three test layers:

| Layer | Where | Stack | What it covers |
|-------|-------|-------|----------------|
| Component tests | `*.spec.tsx` next to the source file | Jest + React Testing Library (RTL) | A single component rendered with props. No data fetching. |
| Page tests | `*.spec.tsx` next to the source file | Jest + RTL | A page-level component composed with its children. SWR hooks are mocked to inject fixture data. |
| End-to-end | `e2e-tests/cypress/e2e/` | Cypress | A real `cockroach` single-node cluster, real browser, real network. See `e2e-tests/README.md`. |

Component and page tests share the same file suffix (`.spec.tsx`) and Jest config — they differ only in scope and whether they mock SWR. Util and API tests (`*.spec.{ts,tsx}`, e.g. `utils.spec.ts`, `*Api.spec.ts`) are plain Jest tests without rendering; use `.ts` for pure-logic tests and `.tsx` only when the test renders JSX.

### When to mock SWR hooks

**Component tests don't fetch data**, so there's nothing to mock — render the component with the props you want and assert directly.

```tsx
render(<Dropdown items={defaultItems} onChange={jest.fn()}>Select</Dropdown>);
expect(screen.getByText("Select")).toBeInTheDocument();
```

**Page tests — mock the SWR hook** so the page renders against a fixture. Use `jest.spyOn` on the hook the page calls and return the full SWR response shape:

```tsx
jest.spyOn(utils, "useSwrWithClusterId").mockReturnValue({
  data: allSchedulesFixture,
  isLoading: false,
  error: null,
  mutate: null,
  isValidating: false,
});
render(
  <MemoryRouter>
    <SchedulesPage {...props} />
  </MemoryRouter>,
);
```

Page tests typically need `MemoryRouter` because hooks like `useHistory` / `useLocation` run during render.

### Cypress

Cypress runs against a real single-node cluster with the `movr` database — no mocking. Structural tests are under `cypress/e2e/pages/` and are run in a nightly TeamCity job.

Cypress tests can also be run locally when in the cockroachdb repo.
Make sure you have a cockroach instance running, and create a user by running the following in SQL shell

```{sql}
CREATE USER cypress WITH PASSWORD 'tests';
```

Then, under `pkg/ui/workspaces/e2e-tests` directory, run

```{shell}
pnpm cy:debug
```
