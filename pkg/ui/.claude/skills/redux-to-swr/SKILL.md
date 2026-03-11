---
name: redux-to-swr
description: Migrate React components from Redux + Saga to SWR hooks. Use when converting data fetching from Redux store (reducers, sagas, selectors, connect HOC) to SWR-based hooks in CockroachDB DB Console or cluster-ui.
---

# Redux + Saga to SWR Migration Guide

Migrate React components from Redux + redux-saga data fetching to SWR (stale-while-revalidate) hooks. The migration eliminates boilerplate (reducers, sagas, selectors, action creators, connected components) and replaces it with co-located data fetching hooks.

Arguments: `$ARGUMENTS`
- First positional arg: path to the component or store directory to migrate (optional)

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
- db-console wrapper simplified to `withRouter(Component)` or functional with `useParams`

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
      refreshInterval: 60 * 1_000,  // optional polling
    },
  );
}
```

```tsx
// Pattern: Hook with conditional fetching (e.g. tenant check)
export const useNodes = () => {
  const { isTenant } = useContext(ClusterDetailsContext);
  const { data, isLoading, error } = useSwrWithClusterId(
    NODES_SWR_KEY,
    !isTenant ? getNodes : null,  // null fetcher = skip
    {
      revalidateOnFocus: false,
      dedupingInterval: 10000,
    },
  );
  // ... derive computed values with useMemo
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
- `refreshInterval` — for data that should poll (dashboards, live lists)
- `revalidateOnFocus: false` — for expensive or rarely-changing data
- `dedupingInterval` — how long to dedup identical requests (default 2s)
- Use `useSWRImmutable` / `useSwrImmutableWithClusterId` for data that never changes for a given key (e.g. historical time series data with a timestamp-based key)

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
- Replace Redux-stored sort/filter state with `useState` + URL sync
- Use the `<Loading>` component with `isLoading` and `error` from SWR

### 3. Simplify the Connected/Wrapper Component

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

**db-console wrapper** — Prefer hooks over HOCs:

```tsx
// Before: connect(mapStateToProps, mapDispatchToProps)(withRouter(Component))
// After: Direct use with hooks
import { ScheduleDetails } from "@cockroachlabs/cluster-ui";
import { withRouter } from "react-router-dom";
export default withRouter(ScheduleDetails);

// Even better (reviewer preference): use useParams/useHistory directly
// and delete the wrapper file entirely if db-console-specific logic
// is not needed.
```

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

### 5. Write Tests

**Preferred pattern: jest.spyOn the hook module**

```tsx
import * as schemaInsightsApi from "../../api/schemaInsightsApi";
import * as userApi from "../../api/userApi";

it("renders expected data", () => {
  const spy = jest.spyOn(schemaInsightsApi, "useSchemaInsights")
    .mockReturnValue({
      data: { results: fixtureData, maxSizeReached: false },
      isLoading: false,
      error: null,
      mutate: null,
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

**Testing pitfall (reviewer nit):** When testing "non-admin" behavior, use a realistic non-admin role (e.g. `["VIEWACTIVITY"]`) rather than an empty array `[]`, since empty roles is an edge case that may not reflect real usage. Use table-driven tests when testing multiple role combinations.

---

## Reviewer Focus Areas & Common Nits

### API / Hook Design

1. **Simplify types when removing Redux indirection.** If a parameter was `string | (() => string)` because the saga needed a thunk, simplify to just `string` now that the hook calls the fetcher directly.

2. **Use `useSWRImmutable`** for data where the key fully identifies the response and the data can't change (e.g. historical metrics with timestamp keys). This avoids revalidation on focus/reconnect/mount.

3. **Don't include `isLoading` in useMemo dependencies** — it causes unnecessary recomputation. Derive loading state outside the memo.

4. **SWR key design:**
   - Simple string for singleton resources: `"getInsightRecommendations"`
   - Object/array for parameterized resources: `{ name: "schedule", id: idStr }`
   - Export the key constant if other hooks need to share the cache: `export const NODES_SWR_KEY = "nodesUI"`
   - SWR supports arrays/objects as keys natively — `JSON.stringify` is usually unnecessary

### Component Structure

5. **Prefer hooks over HOCs.** Replace `withRouter(connect(...)(Component))` with `useParams`, `useHistory`, `useSelector`, `useDispatch` inside the component. Reviewers will request this.

6. **Don't over-abstract.** If a hook is only used by one component, it's fine to keep it simple. Don't create unnecessary wrapper hooks.

7. **Keep URL sync via `useEffect` + `syncHistory`.** This replaces Redux-persisted sort/filter/search state. The pattern:
   - Read initial state from URL params on mount
   - Sync state changes back to URL via `useEffect`

### Cleanup

8. **Remove storybook files** that exist only to provide Redux store context — they're no longer useful after migration.

9. **When removing the connected component,** check if db-console re-exports it. Simplify the db-console side too.

### Testing

10. **Always mock the full SWR return shape** including `mutate` and `isValidating`:
    ```tsx
    { data, isLoading, error, mutate: null, isValidating: false }
    ```

11. **Wrap components in `<MemoryRouter>`** since hooks like `useHistory` and `useParams` require router context. For parameterized routes:
    ```tsx
    <MemoryRouter initialEntries={["/schedules/123"]}>
      <Route path="/schedules/:id">
        <ScheduleDetails />
      </Route>
    </MemoryRouter>
    ```

12. **Verify mock names match actual exports.** A test mocking `useNodeStatuses` when the export was renamed to `useNodes` will silently pass but test nothing. Reviewers watch for this.

### Request Batching (Advanced)

For components where many instances render simultaneously (e.g. metrics graphs), implement a batch fetcher module:
- Queue requests via `requestBatched()`, group by shared parameters
- Flush after the current render cycle via `setTimeout(0)` (not `queueMicrotask` — SWR v2's `useLayoutEffect` callbacks fire across separate microtask windows)
- Split unified responses back to individual callers by query count offset
- Expose `resetBatchState()` for test cleanup

---

## PR Structure

- **Split large migrations** into separate commits/PRs:
  1. Add SWR hooks (API layer)
  2. Convert component to use hooks
  3. Simplify connected/wrapper components
  4. Delete Redux store files
- **PR description** should list what Redux functionality was removed vs preserved
- **Note removed functionality** explicitly (e.g. "removed analytics.track — team agreed low value")

## Composing Hooks

When a component needs data from multiple sources, compose hooks:

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
