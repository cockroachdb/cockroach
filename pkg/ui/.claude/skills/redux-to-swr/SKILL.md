---
name: redux-to-swr
description: Migrate React components from Redux + Saga to SWR hooks. Use when converting data fetching from Redux store (reducers, sagas, selectors, connect HOC) to SWR-based hooks in CockroachDB DB Console or cluster-ui.
---

# Redux + Saga to SWR Migration Guide

Migrate React components from Redux + redux-saga data fetching to SWR (stale-while-revalidate) hooks. The migration eliminates boilerplate (reducers, sagas, selectors, action creators, connected components) and replaces it with co-located data fetching hooks.

---

## Architecture

### Before (Redux + Saga)
```
Component (connected via mapStateToProps/mapDispatchToProps)
  -> dispatch(refresh()) action
  -> saga watches for action, calls API
  -> saga dispatches success/failure actions
  -> reducer updates store
  -> selector reads from store
  -> mapStateToProps feeds data to component
```

Files involved per feature:
- `store/<feature>/index.ts` — barrel export
- `store/<feature>/<feature>.reducer.ts` — Redux reducer
- `store/<feature>/<feature>.sagas.ts` — saga watchers/workers
- `store/<feature>/<feature>.sagas.spec.ts` — saga tests
- `store/<feature>/<feature>.selectors.ts` — selectors
- `store/reducers.ts` — registers slice in combineReducers
- `store/sagas.ts` — forks the saga
- `<component>Connected.tsx` — connect() HOC
- db-console wrapper with connect() / mapStateToProps / mapDispatchToProps

In db-console, many features use `CachedDataReducer` or `PaginatedCachedDataReducer` instead of sagas. These register in `apiReducers.ts` and export `refresh*` functions. The migration approach is the same.

### After (SWR)
```
Component calls useMyData() hook
  -> hook calls useSwrWithClusterId(key, fetcher, options)
  -> SWR handles caching, deduplication, revalidation
  -> component reads { data, error, isLoading } directly
```

Files involved per feature:
- `api/<feature>Api.ts` — API function + SWR hook (co-located)
- `<component>View.tsx` — functional component using hooks directly
- db-console wrapper simplified to a thin functional component or removed entirely

---

## Step-by-Step Migration

### 1. Create the SWR Hook (in `api/<feature>Api.ts`)

Add a hook next to the existing API fetcher function. Use `useSwrWithClusterId` from `src/util` for cluster-ui components.

```tsx
// Pattern: Simple hook wrapping an existing fetcher
export function useSchemaInsights() {
  return useSwrWithClusterId<SqlApiResponse<InsightRecommendation[]>>(
    "getInsightRecommendations",  // stable string key
    () => getSchemaInsights(),     // existing fetcher function
    {
      revalidateOnFocus: false,
      dedupingInterval: 10_000,
    },
  );
}
```

```tsx
// Pattern: Hook with conditional fetching and caller-configurable polling.
// Use null fetcher to skip fetching (e.g. for tenants that lack the endpoint).
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
  return { nodeStatuses: data?.nodes ?? [], isLoading, error };
};

// Usage — overview page polls every 10s, detail pages don't:
const { nodeStatuses } = useNodes({ refreshInterval: 10_000 });
```

```tsx
// Pattern: Conditional fetching based on parameter availability.
// Use null key when the key itself depends on a value that may not be available yet.
export const useNodeLogs = (nodeId: string) => {
  const shouldFetch = Boolean(nodeId);
  return useSwrWithClusterId(
    shouldFetch ? { name: "nodeLogs", nodeId } : null,  // null key = don't fetch
    () => getNodeLogs(nodeId),
    { revalidateOnFocus: false },
  );
};
```

```tsx
// Pattern: Hook with dynamic key (e.g. detail pages)
const { data, error, isLoading } = useSwrWithClusterId(
  { name: "schedule", id: idStr },  // object key, varies per entity
  () => getSchedule(Long.fromString(idStr)),
);
```

**Key decisions for SWR configuration:**
- `refreshInterval` — for data that should poll (dashboards, live lists). Accept as an option so callers can opt in.
- `revalidateOnFocus: false` — for expensive or rarely-changing data
- `dedupingInterval` — how long to dedup identical requests (default 2s)
- Use `useSWRImmutable` / `useSwrImmutableWithClusterId` when data should be fetched once and never automatically revalidated (no revalidation on stale, focus, or reconnect). If another hook mutates the same cache key, the updated value is still reflected — "immutable" refers to the revalidation policy, not the data itself.

**SWR key design:**
- Simple string for singleton resources: `"getInsightRecommendations"`
- Object/array for parameterized resources: `{ name: "schedule", id: idStr }`
- Export the key constant if other hooks need to share the cache: `export const NODES_SWR_KEY = "nodesUI"`
- SWR supports arrays/objects as keys natively — `JSON.stringify` is usually unnecessary

**Simplify types when removing Redux indirection.** If a parameter was `string | (() => string)` because the saga needed a thunk, simplify to just `string` now that the hook calls the fetcher directly.

**Don't over-abstract.** If a hook is only used by one component, it's fine to keep it simple. Don't create unnecessary wrapper hooks.

**Never mutate SWR cached data.** SWR's cache is shared — mutating a cached object (e.g. using `reduce` into `ns.metrics` directly) causes bugs like double-counting. Always spread into a fresh object: `{ ...ns.metrics }`.

**When a hook combines multiple data sources**, see [Composing Hooks](#composing-hooks). **When extracting computation from Redux selectors**, see [Extracting Pure Functions](#extracting-pure-functions-from-selectors).

### 2. Convert the Component to Use Hooks Directly

Replace the connected component pattern with direct hook calls.

**Before:**
```tsx
interface StateProps {
  data: InsightRecommendation[];
  isLoading: boolean;
  error: Error;
}
interface DispatchProps {
  refresh: () => void;
}
type Props = StateProps & DispatchProps;

const SchemaInsightsView: React.FC<Props> = (props) => {
  useEffect(() => { props.refresh(); }, []);
  // uses props.data, props.isLoading, props.error
};

export default connect(mapStateToProps, mapDispatchToProps)(SchemaInsightsView);
```

**After:**
```tsx
export const SchemaInsightsView: React.FC = () => {
  const { data, error, isLoading } = useSchemaInsights();
  const { data: roles } = useUserSQLRoles();
  // ... all state is local via useState
  // ... URL sync via useHistory + useEffect
  return ( /* JSX */ );
};
```

**Key patterns:**
- Replace `mapStateToProps` selectors with direct hook calls
- Replace `useEffect(() => refresh(), [])` dispatch with SWR's automatic fetching
- Replace Redux-stored sort/filter state with `useState` + URL sync via `useEffect` + `syncHistory` (read initial state from URL params on mount, sync changes back to URL)
- Use the `<Loading>` component with `isLoading` and `error` from SWR
- Call hooks and contexts directly instead of threading props from connected wrappers — e.g. `useContext(ClusterDetailsContext)` for tenant info, `useUserSQLRoles()` for roles.
- Check `cluster-ui/src/api/` and `cluster-ui/src/util/` for existing hooks before creating new props or wrappers.
- Don't use `useMemo` for trivial computations — wrapping `parseInt(show, 10)` or simple string operations adds overhead without benefit.

### 3. Simplify the Connected/Wrapper Component

**Prefer hooks over HOCs.** Replace `withRouter(connect(...)(Component))` with `useParams`, `useHistory`, `useSelector`, `useDispatch` inside the component. Note: `useSelector`/`useDispatch` are only for state that genuinely remains in Redux (e.g. `timeScale`, global UI preferences) — data fetching should use SWR hooks.

**cluster-ui connected component** — simplify to re-export or minimal wrapper:

```tsx
// Before: 90+ lines of mapStateToProps, mapDispatchToProps, connect()
// After: Simple re-export or thin wrapper
export const ConnectedIndexDetailsPage: React.FC = () => {
  const { database, table, index } = useParams<RouteParams>();
  const breadcrumbPrefix = useSelector(selectClusterPrefix);
  return (
    <IndexDetailsPage
      databaseName={database}
      tableName={table}
      indexName={index}
      breadcrumbPrefix={breadcrumbPrefix}
    />
  );
};
```

**db-console wrapper** — use hooks, not HOCs:

```tsx
// Before: connect(mapStateToProps, mapDispatchToProps)(withRouter(Component))
// After: functional component with hooks
import { IndexDetailsPageComponent } from "@cockroachlabs/cluster-ui";

export const IndexDetailsPage: React.FC = () => {
  const { database, table, index } = useParams<RouteParams>();
  const timeScale = useSelector(selectTimeScale);
  const dispatch = useDispatch();
  return (
    <IndexDetailsPageComponent
      databaseName={database}
      tableName={table}
      indexName={index}
      timeScale={timeScale}
      onTimeScaleChange={(ts) => dispatch(setGlobalTimeScaleAction(ts))}
    />
  );
};
```

If the db-console wrapper has no db-console-specific logic (no `useSelector`, no `useDispatch`), it can be a simple re-export:
```tsx
export { ScheduleDetails as default } from "@cockroachlabs/cluster-ui";
```

Don't use `useCallback` unnecessarily in connected components — wrapping Redux dispatch calls or simple functions in `useCallback` adds noise. If a function is only used as a `useEffect` dependency, inline the logic in the effect instead.

### 4. Delete Redux Artifacts

Remove these files/registrations:
- `store/<feature>/index.ts`
- `store/<feature>/<feature>.reducer.ts`
- `store/<feature>/<feature>.sagas.ts`
- `store/<feature>/<feature>.sagas.spec.ts`
- `store/<feature>/<feature>.selectors.ts`
- Remove the slice from `store/reducers.ts` (combineReducers)
- Remove the `fork()` from `store/sagas.ts`
- Remove the Redux state type from `AdminUiState` if in db-console
- Remove db-console `redux.ts` / `redux.spec.ts` wrapper files
- Delete storybook files if they only existed to provide Redux context

For db-console `CachedDataReducer` features, also remove:
- The `CachedDataReducer` / `PaginatedCachedDataReducer` instance from `apiReducers.ts`
- The exported `refresh*` function
- The field from `APIReducersState`
- The API function from `util/api.ts` (if only used by the reducer)

**Before removing the db-console API function**, ensure a replacement fetcher exists in `cluster-ui/src/api/<feature>Api.ts`. cluster-ui does not import from db-console — it defines its own fetchers using `fetchData` from `src/api`. Create the cluster-ui fetcher + SWR hook first (Step 1), then remove the db-console artifacts.

**Check for stale selectors.** When removing Redux data fetching, verify that selectors still referencing the old Redux state are also removed or replaced. A selector reading from a store slice that is no longer populated will silently return `undefined`.

**Check db-console re-exports.** When removing a connected component, check if db-console re-exports it and simplify the db-console side too.

### 5. Write Tests

**Always mock the full SWR return shape** including `mutate` and `isValidating`:
```tsx
{ data, isLoading, error, mutate: jest.fn(), isValidating: false }
```

**Verify mock names match actual exports.** A test mocking `useNodeStatuses` when the export was renamed to `useNodes` will silently pass but test nothing.

**Preferred pattern: jest.spyOn the hook module**

```tsx
import * as schemaInsightsApi from "../../api/schemaInsightsApi";

it("renders expected data", () => {
  const spy = jest.spyOn(schemaInsightsApi, "useSchemaInsights")
    .mockReturnValue({
      data: { results: fixtureData, maxSizeReached: false },
      isLoading: false,
      error: null,
      mutate: jest.fn(),
      isValidating: false,
    });

  const { getByText } = render(
    <MemoryRouter>
      <SchemaInsightsView />
    </MemoryRouter>,
  );
  // assertions...
  spy.mockClear();
});
```

**Alternative: jest.mock at module level**

```tsx
jest.mock("../api/nodesApi", () => ({
  useNodes: () => ({
    nodeStatuses: [],
    isLoading: false,
    error: null,
  }),
}));
```

**Test cases to cover:**
1. **Data loaded** — verify content renders
2. **Loading state** — verify loading spinner (`getByTestId("loading-spinner")`)
3. **Error state** — verify error message renders
4. **Role-based visibility** — test with different roles (use table-driven tests for 3+ roles)
5. **Route params** — use `<MemoryRouter initialEntries>` + `<Route path>` for param-dependent components

**Testing hooks directly with `renderHook`:**

```tsx
import { renderHook } from "@testing-library/react-hooks";
import * as nodesApi from "../api/nodesApi";

it.each([
  { nodesLoading: true, expected: true },
  { nodesLoading: false, expected: false },
])("loading state (%o)", ({ nodesLoading, expected }) => {
  jest.spyOn(nodesApi, "useNodes").mockReturnValue(
    { nodeStatuses: [], isLoading: nodesLoading, error: undefined },
  );
  const { result } = renderHook(() => useNodesSummary());
  expect(result.current.isLoading).toBe(expected);
});
```

**db-console test mocking** — mock at the `@cockroachlabs/cluster-ui` module level:

```tsx
jest.mock("@cockroachlabs/cluster-ui", () => ({
  ...jest.requireActual("@cockroachlabs/cluster-ui"),
  useNodesSummary: jest.fn(),
}));

const mockUseNodesSummary = useNodesSummary as jest.Mock;
mockUseNodesSummary.mockReturnValue({
  nodeStatusByID: { "1": mockNode },
  isLoading: false,
  error: undefined,
});
```

**Wrap components in `<MemoryRouter>`** since hooks like `useHistory` and `useParams` require router context. For parameterized routes:
```tsx
<MemoryRouter initialEntries={["/schedules/123"]}>
  <Route path="/schedules/:id">
    <ScheduleDetails />
  </Route>
</MemoryRouter>
```

**Testing pitfall:** When testing "non-admin" behavior, use a realistic non-admin role (e.g. `["VIEWACTIVITY"]`) rather than an empty array `[]`, since empty roles is an edge case that may not reflect real usage. Use table-driven tests when testing multiple role combinations.

### 6. Run Lint Fix

After all code changes are complete, run lint:fix in both workspaces to auto-fix any lint issues introduced during the migration:

```bash
cd pkg/ui/workspaces/cluster-ui && pnpm run lint:fix
cd pkg/ui/workspaces/db-console && pnpm run lint:fix
```

---

## Composing Hooks

When a component needs data from multiple sources, compose hooks. Don't include `isLoading` in `useMemo` dependencies — it causes unnecessary recomputation. Derive loading state outside the memo.

```tsx
// Hook that combines two other hooks
export const useNodesSummary = () => {
  const { nodeStatuses, isLoading: nodesLoading, error: nodesError } = useNodes();
  const { livenesses, statuses, isLoading: livenessLoading, error: livenessError } = useLiveness();

  const isLoading = nodesLoading || livenessLoading;

  const summary = useMemo(() => {
    // derive combined data
  }, [nodeStatuses, livenesses, statuses]);  // NOT isLoading

  return {
    ...summary,
    isLoading,
    error: nodesError ?? livenessError,
  };
};
```

SWR automatically deduplicates — if `useNodes()` is called from both `useNodesSummary()` and another component, only one API call is made (controlled by `dedupingInterval`).

**Error aggregation** — when a component uses multiple hooks, combine errors:
```tsx
// For composed hooks returning a single error:
return { error: nodesError ?? livenessError };

// For components rendering a <Loading> with multiple error sources:
const errors = [nodesError, ddError, livenessError].filter(Boolean);
return <Loading loading={isLoading} error={errors} ... />;
```

### Extracting Pure Functions from Selectors

When Redux selectors contain computation logic (not just state access), extract it into pure exported functions. These are independently testable and composable with `useMemo`:

```tsx
// Before: computation buried in a createSelector
export const selectClusterName = createSelector(
  nodeStatusesSelector,
  livenessSelector,
  (nodeStatuses, liveness) => { /* compute cluster name */ }
);

// After: pure function in cluster-ui, composed with useMemo
// In api/clusterApi.ts:
export function getClusterName(
  nodeStatuses: INodeStatus[],
  livenessStatusByNodeID: Record<string, number>,
): string | undefined {
  // Pure computation — no Redux, no hooks
}

// In hook or component:
const clusterName = useMemo(
  () => getClusterName(nodeStatuses, statuses),
  [nodeStatuses, statuses],
);
```

This enables sharing logic between cluster-ui and db-console, and makes computation testable without mocking hooks or Redux.

---

## Cache Invalidation After Mutations

When a component performs a write operation (POST, DELETE, PUT) and needs to refresh cached data, use the bound `mutate` from the read hook combined with `useSwrMutationWithClusterId` for the write:

```tsx
// Read hook returns bound mutate for manual cache refresh
const { data: detailFiles, mutate: refreshFiles } = useSwrWithClusterId(
  { name: "jobProfilerExecutionFiles", jobID },
  () => listExecutionDetailFiles({ job_id: jobID }),
  { refreshInterval: 10_000 },
);

// Write hook triggers the mutation, then refreshes the read cache
const { trigger } = useSwrMutationWithClusterId(
  { name: "collectExecutionDetails", jobID },
  async () => {
    const resp = await collectExecutionDetails({ job_id: jobID });
    if (resp.req_resp) {
      refreshFiles();  // invalidate the read cache after successful write
    }
  },
);
```

**Key patterns:**
- Destructure `mutate` from the read hook and alias it (e.g. `mutate: refreshFiles`, `mutate: refreshTables`)
- Use `useSwrMutationWithClusterId` (from `src/util`) for write operations — it wraps `useSWRMutation` with cluster ID
- Call the aliased `mutate` after the write succeeds to trigger a revalidation
- Don't use SWR's global `mutate` — prefer the bound `mutate` from the hook that owns the cache key

See `jobProfilerView.tsx` and `getTableMetadataApi.ts` for real examples.

---

## Gradual Migration: SWR-to-Redux Bridge

When existing Redux selectors or alert banners still depend on data that has been migrated to SWR, use a bridge component to sync SWR state into Redux. This avoids rewriting all consumers at once.

```tsx
// Minimal Redux slice to hold the SWR-sourced value
export function healthReducer(state = { lastError: null }, action) {
  switch (action.type) {
    case SET_HEALTH_ERROR:
      return { lastError: action.error };
    default:
      return state;
  }
}

// Invisible bridge component — mounts in the app layout
export function HealthMonitor(): null {
  const { error } = useHealth();
  const dispatch = useDispatch();

  useEffect(() => {
    dispatch(setHealthError(error ?? null));
  }, [error, dispatch]);

  return null;  // renders nothing
}

// In layout:
<HealthMonitor />
<AlertBanner />  {/* still reads from Redux */}
```

Use this pattern sparingly — it's a transitional step. Once all consumers are migrated, remove the bridge and the Redux slice.

---

## PR Structure

- **Split large migrations** into separate commits/PRs:
  1. Add SWR hooks (API layer)
  2. Convert component to use hooks
  3. Simplify connected/wrapper components
  4. Delete Redux store files
- **PR description** should list what Redux functionality was removed vs preserved
- **Note removed functionality** explicitly (e.g. "removed analytics.track — team agreed low value")

---

## Request Batching (Advanced)

For components where many instances render simultaneously (e.g. metrics graphs), implement a batch fetcher module:
- Queue requests via `requestBatched()`, group by shared parameters
- Flush after the current render cycle via `setTimeout(0)` (not `queueMicrotask` — SWR v2's `useLayoutEffect` callbacks fire across separate microtask windows)
- Split unified responses back to individual callers by query count offset
- Expose `resetBatchState()` for test cleanup
