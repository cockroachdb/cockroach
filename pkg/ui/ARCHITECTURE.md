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

There is no separate store, reducer, or saga layer. The hook _is_ the data layer.

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
| `useSwrImmutableWithClusterId(key, fetcher, config?)` | Data that should be fetched once and never automatically revalidated (e.g. grants on a detail page). If another hook mutates the same cache key, the updated value is still reflected. |
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

  // 4. Derive data with useMemo when needed.
  const nodeStatuses = useMemo(
    () => accumulateMetrics(data?.nodes ?? []),
    [data],
  );

  return { nodeStatuses, isLoading, error };
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

All new components should be functional components using React hooks. Components in cluster-ui call SWR hooks directly for data:

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

### Loading and error states

Use the `<Loading>` component from cluster-ui:

```tsx
<Loading
  loading={isLoading}
  error={error}
  page="index details"
  renderError={() => LoadingError({ statsType: "statements", error })}
>
  {/* content rendered when data is available */}
</Loading>
```

For multiple data sources, combine errors:

```tsx
const errors = [nodesError, livenessError].filter(Boolean);
<Loading loading={isLoading} error={errors} page="overview" />
```

### db-console wrapper pattern

db-console wrappers are thin functional components that bridge app-level state (like `timeScale` from Redux) to cluster-ui components:

```tsx
// db-console/src/views/databases/indexDetailsPage/index.tsx
const IndexDetailsPage: React.FC = () => {
  const params = useParams<Record<string, string>>();
  const timeScale = useSelector(selectTimeScale);
  const dispatch = useDispatch();
  return (
    <IndexDetailsPageComponent
      databaseName={params[databaseNameAttr]}
      tableName={params[tableNameAttr]}
      timeScale={timeScale}
      onTimeScaleChange={ts => dispatch(setGlobalTimeScaleAction(ts))}
    />
  );
};
```

If the wrapper has no db-console-specific logic, it can be a simple re-export:

```tsx
export { ScheduleDetails as default } from "@cockroachlabs/cluster-ui";
```

### URL parameters and routing

DB Console uses React Router v5. Use hooks for routing:

- `useParams<RouteParams>()` — extract URL parameters
- `useHistory()` — programmatic navigation
- `useLocation()` — read current URL/search params

Route definitions live in `db-console/src/routes/` and `db-console/ccl/src/routes/`.

---

## App-Level State (Redux)

A small set of concerns are managed in Redux at the db-console level. These are accessed via `useSelector` / `useDispatch` in wrapper components:

| State slice | Purpose | How to access |
|-------------|---------|---------------|
| `timeScale` | Global time window for metrics graphs | `useSelector(selectTimeScale)`, `dispatch(setGlobalTimeScaleAction(ts))` |
| `localSettings` | UI preferences (sort, filter, columns) | `LocalSetting` class with `.selector()` and `.set()` |
| `login` | Authentication state | Selectors in `src/redux/login` |
| `flags` | Feature flags | Selectors in `src/redux/flags` |

New data fetching should **not** use Redux. Use SWR hooks in cluster-ui instead.


---

## Legacy: Redux and CachedDataReducer

Some older parts of the codebase use Redux with `CachedDataReducer` for data fetching. Understanding this pattern is useful when reading or modifying existing code.

### How it worked

```
Component (connected via mapStateToProps/mapDispatchToProps)
  -> dispatch(refresh()) action
  -> CachedDataReducer makes HTTP call, dispatches REQUEST/RECEIVE actions
  -> reducer updates the store
  -> selector reads from store
  -> mapStateToProps feeds data to component via props
```

Files involved per feature:
- `store/<feature>/<feature>.reducer.ts` — Redux reducer
- `store/<feature>/<feature>.sagas.ts` — Saga watchers/workers (if not using CachedDataReducer)
- `store/<feature>/<feature>.selectors.ts` — Reselect selectors
- Connected component using `connect(mapStateToProps, mapDispatchToProps)`

### CachedDataReducer

`CachedDataReducer` is an internal library that managed API calls and tracked their lifecycle (loading, success, error) in Redux state. Instances were registered in `apiReducers.ts` and exported `refresh*` functions. The pattern is uniform but involves significant boilerplate compared to a single SWR hook call.

### Concrete example: Statements Page (legacy flow)

Here's how the Statements Page historically loaded data:

1. React calls `render()` then `componentDidMount()` on `StatementsPage`
2. `StatementsPage` dispatches `refresh()` via its connected props
3. `cachedDataReducer.ts/refresh` dispatches a `REQUEST` action and makes an HTTP call
4. The reducer sets `inFlight: true` in the store
5. On response, a `RECEIVE` action updates the store with the payload
6. Redux merges the new state and triggers recomputation of selectors
7. `selectStatements` (a memoized `createSelector`) produces processed data
8. The selector output is bound to the component's props via `connect()`, triggering a re-render

### What remains

A small number of components (approximately 13) still use the `connect()` pattern. When modifying these files, prefer converting to hooks (`useSelector`, `useDispatch`, and SWR hooks) rather than extending the legacy pattern. The data fetching should move to an SWR hook in `cluster-ui/src/api/`, and the db-console wrapper should become a thin functional component.
