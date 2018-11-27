- Feature Name: Settings Table
- Status: completed
- Start Date: 2017-03-17
- Authors: David Taylor, knz, ben
- RFC PR: [#14230](https://github.com/cockroachdb/cockroach/pull/14230),
          [#15253](https://github.com/cockroachdb/cockroach/pull/15253)
- Cockroach Issue: [#15242](https://github.com/cockroachdb/cockroach/issues/15242)

# Summary

A system table of named settings with a caching accessor on each node to provide
runtime-alterable execution parameters.

**How to use, tl'dr:**

- use one of the `Register` functions to create a tunable setting in your code.
- we should never have an env var (or command-line flag) and a cluster setting for the same thing.
- settings names are not case sensitive and must be valid SQL
  identifiers, so `sql.metrics.statement_details.enabled` is good but
  `sql.metrics.statementDetails.enabled` or
  `sql.metrics.statement-details.enabled` isn't. We use dots for
  hierarchy, and the last part(s) of the name must indicate clearly
  what the value is about.

# Motivation

We have a variety of knobs and flags that we currently can set at node startup,
via env vars or flag. Some of these make sense to be able to tune at runtime,
without requiring updating a startup script or service definition and subsequent
full reboot of the cluster.

Some current examples, drawn from a cursory glance at our `envutil` calls, that
might be nice to be able to alter at runtime, without rebooting:

* `COCKROACH_SCAN_INTERVAL`
* `COCKROACH_REBALANCE_THRESHOLD`
* `COCKROACH_LEASE_REBALANCING_AGGRESSIVENESS`
* `COCKROACH_CONSISTENCY_CHECK_INTERVAL`
* `COCKROACH_MEMORY_ALLOCATION_CHUNK_SIZE`
* `COCKROACH_NOTEWORTHY_SESSION_MEMORY_USAGE`
* `COCKROACH_DISABLE_SQL_EVENT_LOG`
* `COCKROACH_TRACE_SQL`

Obviously not all settings can be, or will even want to be, easily changed
at runtime, at potentially at different times on different nodes due to caching,
so this would not be a drop-in replacement for all current flags and env vars.
For example, some settings passed to RocksDB at startup or those affecting
replication and internode interactions might be less suited to this pattern.

# Detailed design

A new system.settings table, keyed by string settings names would be created.

```
CREATE TABLE system.settings (
  name STRING PRIMARY KEY,
  value STRING,
  updated TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  valueType char NOT NULL DEFAULT 's',
)
```

The table would be created in the system config range and thus be gossiped. On
gossip update, a node would iterate over the settings table to update its
in-memory map of all current settings.

A collection of typed accessors fetch named settings from said map and marshal
their value in to a bool, string, float, etc.

Thus retrieving a setting from the cache _does not_ have any dependencies on a
`Txn`, `DB` or other any other infrastructure -- since the map is updated
asynchronously by a loop on `Server` -- making it suitable for usage at a broad
range of callsites (much like our current env vars).

While (thread-safe) map access should be relatively cheap and suitable for many
callsites, particularly performance-sensitive callsites may instead wish to use
an accessor that registers a variable to be updated atomically on cache refresh,
after which they can simply read the viable via one of the sync.atomic
functions.

Only user-set values need actually appear in the table, as all accessors provide
a default value to return if the setting is not present.

## Centralized Definition

A central list of defined settings with their type and default value provides
the ability to:

* list all known-settings
* validate access a given setting uses the appropriately typed accessor
* validate writes to a setting are of the appropriate type

## Modifying Settings

The `SET` statement will optionally take a `CLUSTER SETTING` modifier to specify
changes to a global setting, e.g.
  `SET CLUSTER SETTING storage.rebalance_threshold = 0.5`

The `settings` table will not have write privileges at the SQL layer, but will
instead be read-only, and only readable by the root user, thus forcing the use
of the `SET` statement, ensuring validation and allowing for changes to be in a
`settings_history` table (e.g. with a`(name,time)` key and the old value).

# Settings vs. env vars

**We must ensure that users have a consistent experience of
configuration, and not make different mechanisms for different
configuration options, which would cause endless confusion and
complexity in docs.**

## Let's not allow any overlap

Suppose we had both an env var and a setting for the same tuning
knob. Then two possible situations would arise:

- env var has priority (override over the setting): this can be useful
  to override the setting for a specific node. For example we could
  have some general behavior set via the setting, and the use env var
  to do some local testing using a custom value on one (or several
  nodes) without impacting the rest of the cluster.

  However it would confuse users that run `SHOW ALL CLUSTER SETTINGS` or
  use the admin endpoint for settings, as the resulting information
  could be inaccurate on some nodes, without any indication that it is
  inaccurate.

- setting has priority (override over the env var): conceptually this
  is as if the env var was the "default value" for the setting until
  the setting is set. This has weird semantics if the env var doesn't
  have the same value on every node: until the setting is set, after
  which it will have the same value everywhere, the nodes can observe
  different things.

**These two situations are both undesirable** because for each of them
the downside is just poor user experience.
So we suggest instead that nothing is configurable using both mechanisms.

## Proposed guidelines (from Ben)

- Anything that we want to document for users should be either a
  cluster setting or a command-line flag, with the former strongly
  preferred. Flags should be used only for things that need to vary
  per-node (like cache size) or are impractical to make a cluster
  setting (like max offset or `--join`).

- Environment variables are OK as a quick way to make something
  customizable for our own testing, but we should try to minimize
  this, and they should probably be temporary in most cases. (in the
  long term we may want to either introduce "hidden" cluster settings
  or just have an internal "here be dragons" namespace for these so we
  don't have to use env vars for them. But using cluster settings also
  implies that things may change at runtime, and that's not always
  easy to do)

- Sometimes it's appropriate for the same thing to be both a cluster
  setting and a session variable; in this case I think the session
  variable would always take precedence. I think this would be the
  only time we'd want to have the same variable set at two different
  levels.

## Session vars vs cluster settings

Usually this is not difficult to decide -- either something that needs
to change per session (or per user) or something global for the
cluster. But sometime the question arises, for example as of this
writing what do we do with the "distsql" flag? (#15045)

**Proposed pattern:** session var at highest priority (session var
always decides), but upon creating a new session the session var is
initialized from something else, for example the cluster setting.


## Q & A cluster settings vs env vars

- What if we need to disable a session var value or something for
  testing/debugging? How to prevent clients from customizing the
  session default? If that need arises, then the mechanism would be to
  add a gate flag on session init and set session var (not set cluster
  setting) to prevent said session var to be configured in a specific
  way if some condition is met, presumably some debug knob.

- What if we want to provide a non-default value for a setting that
  impacts cluster initialization? (Asked in
  https://github.com/cockroachdb/cockroach/issues/15242#issuecomment-296224536
  ) - we should provision a way to set up the cluster settings upfront
  for newly created clusters. Ben: "Another thing for the explicit
  init command, perhaps." (see RFC merged in #14251
  [init_command.md](20170318_init_command.md))

# How to name cluster settings

"There are only three hard problems in computing science: naming
things and off-by-one errors."

*So say you want to name that configuration flag, what names should
you give it?*

## Current state of things

As explained above we have a cluster-wide configuration
system with a shared namespace.
At the time of this writing there are already a couple configurable
things this way, and a list of about fifty more to come.

## Proposed consensus

Based on examples:

```
kv.snapshot.recovery.max_rate
```

- `kv`: the top-level architecture layer in CockroachDB
- `snapshot.recovery`: the tuning knob
- `max_rate`: the impact/meaning of the value of the setting

```
sql.trace.session.eventlog.enabled
sql.trace.txn.threshold
```

- `sql`: the top-level architecture layer
- `trace`: the sub-system that's being configured (tracing)
- `session.eventlog`, `txn`: the tuning knob / part
- `enabled`, `threshold`: the meaning of the value

That gives us the general structure:

**overall-layer dot thing-being-configured dot impact/meaning**

## Multiple words

- Dot for hierarchy (overall-layer, thing-being-configured, impact-meaning)
- lowercase with underscores between words
  - we do not use camelCase because **we want to keep the settings case-insensitive**
  - **no special SQL characters or punctuation**, we need to keep the structure of
    a SQL identifier so that `SET CLUSTER SETTING ... = ` can be
    parsed without problem - so no dashes (`blah_blah` not `blah-blah`)
  - use **full words** or very common abbreviations so that the setting can be spoken over audio

## Naming the last part

- `max_rate`: max bytes/second
- `enabled`: for a feature
- `threshold`: for things like min value before something happens

## Naming boolean things

A name like `session.logging.enabled` sounds right, whereas a name
like `session.show_log.enabled` sounds a bit awkward/verbose. What's
going on exactly?

This is a matter of grammatical structure:

- if the thing that's being configured is a feature **noun**, then
  it's not clear what a boolean value would do to it. So what comes
  afterwards must be "enabled" to clarify.
- if the thing is described by a **verb**, then a boolean implicitly
  says "do" or "do not do" that verb.

This is what Mozilla has adopted, compare:

- "Noun flags": `layers.async-pan-zoom.enabled`, `media.peerconnection.enabled`, `security.insecure-password.ui.enabled`
- "Verb flags": `alerts.showFavIcons`, `accessibility.warn_on_browsewithcaret`, `layers.acceleration.draw-fps`

## Settings that both have an on/off switch and a value

For example SQL statement tracing has both an on/off flag and if it is
on, it's only activated if the statement latency exceeds some
threshold.

Two approaches initially considered:

1. a single scalar setting `threshold` with a note in the description "if the value is 0, it means tracing is disabled; use 1ns to enable for all statements"
   - The special casing here feels inelegant.
2. two settings side by side, one `enabled` boolean and one `threshold` only applicable if the `enabled` setting is true.
   - This is more complicated to explain to the user.

Ideally what we want is an "option type" for settings, where the
special string "disabled" is a valid value for a scalar setting, and
the Go code can enquire whether the setting is Enabled/Disabled
besides obtaining its value with `Get()`. We may implement this in the
future, and for the time being we keep on Go's "useful defaults" or
"useful zeros" philosophy which means adopting choice 1 above.

## Settings that have enumerated values

Example use case: "For `distsql.mode` for example, we need the string to
be one of auto, always, on, off. But if it's malformed, interpreting
it as Auto by default is very unsatisfying, as its too late. We need
the set distsql.mode = offf (sic) to fail on set time, rather than
silently succeeding. "

Solution: Use the special type `EnumSetting` where you can specify the
enumeration of allowable values upfront, and where the setting
subsystem will validate user inputs with `SET`.

## Some context for understanding why this matters

How much we should think about settings name is really a design
spectrum.

At one end of the spectrum, you could simply name a new setting with a
random string of numbers. Or your name followed by the current
date. Some immediate reasons why this is a bad idea:

- if you find this setting already configured some time later, you
  won't remember what it means.
- when you ask someone to list all the settings they have currently
  configured, it's going to be a lot of work to figure out what their
  configuration really is.
- if you tell someone to configure something over the phone, or with
  your handwritings, chances are they will spell something wrong and
  configure something else than they intended.

So naming should satisfy a couple high-level criteria:

- it must use words that can be shared in an audio conversation;
- when found on its own it must give some idea of where to look to get
  more information about what it configures.

At the complete end of the design spectrum we have a committee of 3+
linguists that analyze all the code around the setting being created,
analyze the possible names that will unambiguously refer to the thing
and check in 10+ different human languages with random user trials
that it won't be misunderstood. That would give very good names,
likely, but also be very expensive money- and time-wise.

So right now we're considering a flat namespace with conventions
new names. That's like Mozilla.

## What others do

- Mozilla: single namespace, happy-go-lucky name soup with some conventions.
- Windows registry: top-level sections determined by vendor, name soups in each sections with no structure
- FreeBSD (/etc/rc.conf): programname-underscore-configflag
- GConf: top-level sections determined by vendor, lots of committee
  work to organize the hierarchy in a way that's interoperable between
  apps


# Alternatives

## Per-row TTLs

Rather than letting nodes cache the entire table, individual rows could instead
have more granular, row-specific TTLs. Accessors would attempt to fetch and
cache values not currently cached. This would potentially eliminate
false-negatives immediately after a setting is added and allow much more
granular control, but at the cost of introducing a potential KV read. The added
calling infrastructure (a client.DB or Txn, context, etc), combined with the
unpredictable performance, would make such a configuration provider suitable for
a much smaller set of callsites.

## Eagerly written defaults

If we wrote all settings at initialization along with their default values, it
would make inspecting the in-use values of all settings, default or not,
straightforward, i.e `select * from system.settings`.

Doing so however makes updating defaults much harder -- we'd need to handle the
migration process while taking care to avoid clobbering any expressed settings.

Obviously eagerly written defaults could be marked on user-changes and we could
add migrations when adding and changing them, but this adds to the engineering
overhead of adding and using these settings. Additionally, we can still get the
easy listing of in-use values, if/when we want it, by keeping a central list of
all settings and their defaults.
