---
name: db-console-feature
description: Add a new feature-flagged page to the DB Console. Use when creating a new DB Console page that should be gated behind a cluster setting toggle.
---

# Adding a DB Console Feature

New DB Console pages can be added behind a `dbconsole.feature_flag.<name>`
cluster setting. When enabled, the page appears in the sidebar; when disabled,
it's hidden and its backend endpoints return 403.

## How it works

Read `pkg/server/dbconsole/features.go` for the `Feature` struct and registry.
The `jobs_manager.go` file in the same package is a working example.

## Steps to add a new feature

### 1. Backend: create a feature file

Create `pkg/server/dbconsole/<feature_name>.go`:

```go
package dbconsole

func init() {
	RegisterFeature(&Feature{
		Name:        "my_feature",      // cluster setting key suffix
		Title:       "My Feature",      // sidebar display text
		Description: "Does something",  // shown on the feature flags debug page
		RoutePath:   "/feature/my-feature", // frontend route path
	})
}
```

`RegisterFeature` automatically creates the cluster setting
`dbconsole.feature_flag.my_feature` (default: false).

### 2. Backend: add BFF endpoints

Add handler methods on `ApiV2DBConsole` in the same file. Guard each with:

```go
if !requireFeatureEnabled(ctx, "my_feature", &api.Settings.SV, w) {
    return
}
```

### 3. Backend: register routes

Add your routes to the `Handler()` method in `dbconsole.go`:

```go
mux.HandleFunc("/my-feature/data", api.GetMyFeatureData)
```

### 4. Frontend: create the page component

Create `pkg/ui/workspaces/db-console/src/views/myFeature/index.tsx` as a
functional component. Use SWR hooks for data fetching (see `useHotRanges.ts`
for the pattern). API paths must not have a leading `/`.

### 5. Frontend: register the component

In `app.tsx`, add your component to the `featureComponents` map:

```typescript
import MyFeaturePage from "src/views/myFeature";

const featureComponents: Record<string, React.ComponentType> = {
  "my-feature": MyFeaturePage,
  // ...
};
```

The slug key (`"my-feature"`) must match the last segment of `RoutePath`.

### 6. Build

```bash
./dev generate bazel
./dev build pkg/server/dbconsole
./dev test pkg/server/dbconsole -v --count=1
```

## Toggling features

- **SQL:** `SET CLUSTER SETTING dbconsole.feature_flag.my_feature = true`
- **UI:** Advanced Debug > Feature Flags page (`/debug/feature-flags`)

## Key files

- `pkg/server/dbconsole/features.go` — registry, guard, list/toggle endpoints
- `pkg/server/dbconsole/jobs_manager.go` — example feature implementation
- `pkg/ui/workspaces/db-console/src/hooks/useFeatures.ts` — frontend SWR hook
- `pkg/ui/workspaces/db-console/src/views/featureFlags/index.tsx` — debug page
- `pkg/ui/workspaces/db-console/src/app.tsx` — `featureComponents` map
