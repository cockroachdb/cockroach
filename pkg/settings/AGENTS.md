## At a glance

- **Package**: `pkg/settings/` — cluster settings definitions, registration, values container, and update plumbing.
- **Owners**: Server (settings infrastructure)
- **Primary responsibilities**: define typed settings with defaults and validation; persist/encode values; apply changes from storage/overrides; notify watchers; enforce tenant scoping and visibility.
- **Key entry points & types**:
  - `RegisterBool/Int/Float/String/Duration/ByteSize/Enum/ProtobufSetting` — declare settings with defaults and options (validation, visibility).
  - `VersionSetting` — special type for cluster version gate; coordinated via `pkg/clusterversion`.
  - `settings.Values` — per-process container of current values and change callbacks; specialized for system vs virtual cluster.
  - `settings.Updater` — applies updates from `system.settings` and external overrides; tracks `ValueOrigin`.
  - `Setting.SetOnChange` — register fast, non-blocking callbacks for dynamic changes.
  - Watchers: `pkg/server/settingswatcher/` and `pkg/server/tenantsettingswatcher/` — stream changes and feed `Updater`.
  - Feature flags: `pkg/featureflag/` uses `BoolSetting` gates; see `featureflag.CheckEnabled`.
- **Critical invariants**
  - Typed defaults are validated at registration; runtime updates are validated by type-specific decoders and options.
  - `SetOnChange` callbacks run on the single settings update goroutine — must be non-blocking and idempotent.
  - Tenant scoping is enforced in `settings.Values`: SystemOnly settings are inaccessible in virtual clusters.
  - Cluster version is not applied via the generic watcher path; it is coordinated by `clusterversion`.
 - **See also**: `pkg/upgrade/AGENTS.md`, `pkg/multitenant/AGENTS.md`, `pkg/sql/AGENTS.md`, `pkg/kv/kvserver/AGENTS.md`
- **Flow (read + change)**
```
system.settings + overrides  ->  settingswatcher  ->  settings.Updater  ->  settings.Values  ->  SetOnChange callbacks
                                             ^                                 ^
                                             |                                 |
                                  tenantsettingswatcher (for multi-tenant propagation)
```

### Deep dive

1) Architecture & control flow
- Registration: each setting is declared once at init using a `Register*Setting` helper that captures the type, default, class (tenant scope), description, and options (validation, visibility, unsafe, aliases).
- Storage & encoding: user-visible name (`SettingName`) differs from storage key (`InternalKey`). Values are encoded strings typed by a one-letter `Typ()` discriminator (e.g., `b`, `i`, `s`, `d`, `z`, `f`, `e`, `p`, `m`).
- Process state: `settings.Values` holds current values, per-process callbacks, and default overrides. It is specialized at startup to system interface or virtual cluster, controlling access to SystemOnly settings.
- Updates: `settingswatcher.SettingsWatcher` streams changes from the `system.settings` table via rangefeed and calls `Updater.SetFromStorage`. External overrides (e.g., host→tenant) are provided by an `OverridesMonitor` and applied with origin `ExternallySet`.
- Notification: when a setting’s value changes in `Values`, the package invokes all registered `SetOnChange` callbacks for that setting.

2) Data model & protos
- Defaults are declared in registration and surfaced via `DefaultString()` and `EncodedDefault()`.
- Value origins:
  - `OriginDefault` — built-in default (possibly with a default override).
  - `OriginExplicitlySet` — persisted in `system.settings` via `SET/RESET CLUSTER SETTING`.
  - `OriginExternallySet` — an operator/host override (e.g., system tenant override for secondary tenants).
  - `OriginOverride` — process-local test override via the `Override` helpers; preserved across `Updater.ResetRemaining`.
- Default overrides: the process can change the effective default (without forcing an active value) by writing to a per-setting “default override” slot; used to propagate system-visible defaults from the system tenant to secondary tenants while preserving virtual-cluster explicit choices.
- Validation: per-type `WithValidate*` options enforce constraints (e.g., non-negative, max, custom string/proto/enum validators). Invalid updates are rejected and the previous value is kept.
- Safety valve: env `COCKROACH_IGNORE_CLUSTER_SETTINGS=true` makes `Updater` a no-op, keeping values at defaults/overrides for safe bring-up or emergency operation.

3) Concurrency & resource controls
- The `settingswatcher` rangefeed maintains an in-RAM cache of `system.settings`, persists a snapshot for restart, and applies updates through `Updater` in timestamp order (avoids regressions on watcher restarts).
- For SystemVisible settings, the system tenant also sends “alternate defaults” to tenants via `tenantsettingswatcher`, ensuring tenant processes align with the system tenant’s default even if binaries differ.
- `SetOnChange` is invoked synchronously on the watcher’s update goroutine. Guidance:
  - Keep callbacks fast, non-blocking, and idempotent; do not perform I/O or heavy work inline.
  - Prefer signaling a buffered channel to a background worker that applies changes.
  - Re-read the setting inside the callback to obtain the final, de-duplicated value.
  - Tolerate missed intermediate values; only the last applied value matters for many knobs.

4) Mixed-version / upgrades
- The cluster version is modeled by `VersionSetting` but is not applied via the generic settings path; `pkg/clusterversion` coordinates validation, RPC propagation, and activation.
- Feature/behavior checks should use `st.Version.IsActive(ctx, clusterversion.XYZ)` before enabling behaviors; avoid coupling feature toggles to generic settings during upgrades.
- `SET CLUSTER SETTING version = crdb_internal.node_executable_version()` is guarded: resets are disallowed; upgrades validate min/max and perform migrations.
- During mixed-version operation, SystemVisible defaults from the system tenant keep tenant default behavior compatible until all nodes upgrade.

5) Configuration & feature gates
- Class determines who can read/write and where the value is needed:
  - `SystemOnly` — storage/KV-only; not visible or accessible from SQL pods; shared across all tenants; cannot be set from virtual clusters.
  - `SystemVisible` — storage/KV-controlled but visible to SQL; read-only in tenants; propagated as alternate defaults from the system tenant.
  - `ApplicationLevel` — per-tenant setting; readable and settable by virtual clusters; never used by KV/storage.
- `Values.SpecializeForVirtualCluster()` marks SystemOnly settings forbidden; test builds panic on misuse to catch leaks across layers.
- Operators can force read-only behavior for an ApplicationLevel setting inside a tenant by setting a system override (so the tenant observes an external origin value).

- Feature flags: prefer `pkg/featureflag/` for simple user-visible gates; keep flags ApplicationLevel unless KV/storage depends on them.

6) Interoperability
- Settings are consumed across SQL, KV, and storage layers; the system tenant propagates defaults/overrides to virtual clusters.
- Version gates are checked via `clusterversion` across components; avoid gating inbound behavior on version.

7) Failure modes & recovery
- If the watcher is not yet initialized, subsystems should behave as if defaults are in effect; avoid assuming values are present at process start.
- On invalid or unknown keys, updates are ignored; retired settings are tolerated.
- Persisted snapshot enables fast recovery of last-seen values on restart before KV becomes available.

8) Observability & triage
- `SHOW [ALL] CLUSTER SETTINGS` reads from the current process state; non-reportable and sensitive settings are redacted or hidden per `WithReportable` and `Sensitive` options.
- `ValueOrigin` can be inspected per setting at runtime to understand whether a value comes from default, explicit, external override, or test.
- Logs: settingswatcher emits Dev logs for initial scan, update application, and override propagation.

9) Edge cases & gotchas
- Do not block in `SetOnChange`; avoid deadlocks by not calling code that indirectly awaits settings updates.
- For SystemVisible settings, tenants receive alternate defaults; if a tenant has an explicit ApplicationLevel value, that value wins over the default.
- Unsafe settings (`WithUnsafe`) require explicit interlocks and must not be `Public`.

10) Examples
- Tiny example — register and observe changes:
```
var MySetting = settings.RegisterBoolSetting(settings.ApplicationLevel, "my.bool", "desc", false)
MySetting.SetOnChange(&sv.Settings, func(ctx context.Context) { _ = MySetting.Get(&sv) })
```
11) References
- `pkg/server/settingswatcher/` — storage-backed watcher.
- `pkg/server/tenantsettingswatcher/` — multi-tenant override/default propagation.
- `pkg/clusterversion/` — version keys and gates used throughout the codebase.
- `pkg/featureflag/` — standardized UX gate on top of settings.
- `pkg/upgrade/AGENTS.md` — cluster version bumps and migrations.

12) Glossary
- SystemOnly: storage/KV-only setting; invisible to tenants; cannot be set from virtual clusters.
- SystemVisible: storage/KV-controlled but visible to SQL; read-only in tenants; defaults propagated.
- ApplicationLevel: per-tenant setting; readable/settable in tenants; never used by KV/storage.
- VersionSetting: special setting coordinating cluster version activation via `pkg/clusterversion`.
- Values: per-process container of effective setting values and change callbacks.
- Updater: applies changes from storage and overrides; records each value’s `ValueOrigin`.
- ValueOrigin: one of Default, ExplicitlySet, ExternallySet, Override (test-local).
- SetOnChange: fast, non-blocking callback invoked synchronously on the update goroutine.
- OverridesMonitor: source of external overrides (e.g., system→tenant defaults/overrides).
- SettingsWatcher/TenantSettingsWatcher: watchers that stream changes to `Updater`.

13) Search keys & synonyms
- VersionSetting, clusterversion
- settingswatcher, tenantsettingswatcher
- settings.Values, settings.Updater
- OverridesMonitor
- Setting.SetOnChange
- ValueOrigin
- SystemOnly, SystemVisible, ApplicationLevel
Omitted sections: Background jobs & schedules

