- Feature Name: Bundle JS with esbuild
- Status: draft
- Start Date: 2022-01-12
- Authors: Sean Barag (@sjbarag)
- RFC PR: Pending (PR # after acceptance of initial draft)
- Cockroach Issue: None

# Summary

Builds of CockroachDB that include the HTML/CSS/JavaScript UI served on
port 8080 currently spend between 30 s (under `make -j16`) and 131 s
(under `bazel build --config=with_ui`) bundling that UI into a series of
files fit for serving.  While the existing [webpack](webpack.js.org/)
bundler is the defacto standard in the JS community, its speed
limitations are beginning to be a significant limitation on the speed at
which CRDB builds complete. Improving JS bundling time (i.e. the three
`webpack` commands that run today) will improve developer efficiency,
reduce CI build duration (which should reduce CI costs), and will
generally support a faster feedback loop for anyone working with CRDB.
More modern bundlers &mdash; especially the proposed
[esbuild](esbuild.github.io/) &mdash; significantly outperform webpack
while still meeting our needs, and should be considered as viable
alternatives.  End-users should notice no difference in experience, but
anyone building CockroachDB with the UI should notice a significant
speed-up.

# Motivation

The motivation here is to be more efficient with computing resources so
devs can build CRDB more often, or at least so they can wait less time
between builds.  JS build time is consistently mentioned as a developer
pain-point, and we have the power to solve this.

I know of no PMs for this space because it's so internally-facing, but
some user-stories I can think of are:

- As a CockroachDB Go developer who builds the JS UI, I want to
  minimize the time spent waiting for webpack or similar JS bundlers so
  I can focus on my changes.
- As a CockroachDB JS developer, I want to minimize the time spent
  waiting for webpack or similar JS bundlers so I can iterate on my JS
  changes more rapidly.
- As a CockroachDB infrastructure maintainer, I want to minimize the
  specs of each CI build worker to reduce costs.
- As a CockroachDB user who builds from source, I want my initial
  interactions with CockroachDB to be well-optimized.

Initial benchmarking for a build of the `pkg/ui/cluster-ui/` tree under
both webpack and esbuild shows that esbuild bundling completes in ~12%
the time of a webpack build while producing a smaller JS bundle:

- Webpack:
  - Duration: 30 s
  - Bundle size: 7.06 MB
- esbuild:
  - Duration: 3.1 s
  - Bundle size: 6 MB

# Technical design

The simplest way to think of this is `s/webpack/esbuild/` &mdash; that
is, replace all uses of webpack with an equivalent esbuild command.
Many of the complications included in our existing webpack configuration
are a symptom of webpack's slowness today (e.g. the presence of a
"vendor" bundle, the need for "cache-loader", etc.), and esbuild's
significant speed improvement means those techniques are either
unnecessary or increase build durations.  With that in mind, there will
be some conceptual differences in the output but nothing so significant
that the entire app must change.

## Prompts

### Questions about the change

- What components in CockroachDB need to change? How do they change?

  - Any JS components that import from [antd
    3.x](https://3x.ant.design/) will need to manually import both the
    JS and associated style

    Our current webpack build relies on [a babel
    plugin](https://github.com/umijs/babel-plugin-import) to convert one
    import into two:

    ```ts
    // Committed source:
    import { Button } from "antd";

    // Transformed source, after babel plugin's transform:
    import Button from "antd/lib/button";
    import "antd/lib/button/style";
    ```

    Since Babel isn't involved in an esbuild approach, and writing such
    a plugin would be [quite slow in
    esbuild](https://github.com/evanw/esbuild/issues/1763), we can
    simply make the transformed source our convention.  This removes
    some "magic" from the build system at the cost of having to remember
    to "do two things".

    ```ts
    // New committed source:
    import Button from "antd/lib/button";
    import "antd/lib/button/style";
    ```
  - Import paths for some .scss files become relative to the importing
    file

    Similar to the antd imports above, the exsting TypeScript sources
    often import a `.scss` file using a path that starts with `src/`.
    This requires custom path resolution behavior in both esbuild and
    its SASS plugin, and gets quite confusing.  We can convert these
    imports to standard relative paths, thus requiring no transform at
    build time:

    ```ts
    // Old committed source:
    import sortedTableStyles from "src/sortedtable/sortedtable.module.scss";

    // New committed source:
    import sortedTableStyles from "../sortedtable/sortedtable.module.scss";
    ```
  - The main UI server entry point: [pkg/ui/ui.go](../../pkg/ui/ui.go)

    If any net-new files are emitted during this migration, or existing
    files are removed, we'll need to modify the index.html template in
    pkg/ui/ui.go.  This may also take the form of adding a `<link/>` tag
    to load CSS as part of page load, rather than as part of the JS
    process.
  - Makefiles and BUILD.bazel files that touch the existing UI build
    system

    Naturally, anything that currently references webpack will need to
    reference call out to esbuild (or execute an esbuild.js entrypoint)
    instead.  The existing Bazel dependency tree for source files will
    be largely unchanged.

- Are there new abstractions introduced by the change? New concepts?
  - No new abstractions or concepts.

- How does this work in a multi-tenant deployment?
  - No impact: the UI is built and embedded into each instance of
    CockroachDB.

- How does the change behave in mixed-version deployments? During a
  version upgrade? Which migrations are needed?
  - N/A: No schema changes involved, and the UI continues to be embedded
    in each instance.

- Is the result/usage of this change different for CC end-users than
  for on-prem deployments? How?
  - No - all users continue get the same JS output.

- What are the possible interactions with other features or sub-systems
  inside CockroachDB? How does the behavior of other code change
  implicitly as a result of the changes outlined in the RFC?

  The `pkg/ui/cluster-ui` tree is currently used in both CockroachDB and
  in the UI served via https://cockroachlabs.cloud.  That tree is
  currently bundled (and published to NPM) as a [UMD
  module](https://github.com/umdjs/umd), but esbuild only supports
  producing CommonJS and the more modern ES Modules formats. Migrating
  to esbuild may break backwards compatibility for third-party users who
  include @cockroachlabs/cluster-ui in their projects if they aren't
  able to support the CommonJS format. Luckily, [there are no public
  consumers of that
  package](https://www.npmjs.com/browse/depended/@cockroachlabs/cluster-ui).

- Is there other ongoing or recent RFC work that is related?
  - [Cluster UI Code Organization](./20210415_db_console_cluster_ui.md):
    this RFC maintains the status quo of that organization pattern
  - [DB Console Bazel Build](./20210423_db-cosole-bazel-build.md): this
    RFC augments that approach, mostly by replacing the webpack steps
    with equivalent esbuild ones

- What are the edge cases? What are example uses or inputs that we think
  are uncommon but are still possible and thus need to be handled? How
  are these edge cases handled? Provide examples.
    - As always, building CockroachDB on Windows should be tested before
      this RFC is implemented and merged.
    - Use of a WIP `cluster-ui` package in the
      https://cockroachlabs.cloud project is pretty common and must
      continue to be supported, but as long as esbuild continues to
      produce equivalent files there should be no issues.

- What are the effect of possible mistakes by other CockroachDB team
  members trying to use the feature in their own code? How does the
  change impact how they will troubleshoot things?

  The biggest risk is forgetting to import an antd component's style in
  addition to the component itself.  This would present as a mostly
  unstyled component, however, and would quickly be discovered during
  feature development.  Troubleshooting will mostly take the form asking
  around on Slack or comparing to other files, though it's likely we
  could write an `eslint` rule to detect this.

### Questions about performance

- Does the change impact performance? How?

  Build performance should improve significantly, using roughly 10-20%
  of the currently bundling time.  Runtime performance of the database
  is unaffected (with slight improvements possible due to a smaller JS
  bundle being served over HTTP), and runtime performance of the JS
  itself should be unaffected as well.  It's possible that esbuild's
  minifier will produce less efficient code than webpack's minifier.

- If new algorithms are introduced whose execution time depend on
  per-deployment parameters (e.g. number of users, number of ranges,
  etc), what is their high-level worst case algorithmic complexity?

  No new algorithms are introduced.

- How is resource usage affected for “large” loads? For example, what do
  we expect to happen when there are 100000 ranges? 100000 tables? 10000
  databases? 10000 tenants? 10000 SQL users?  1000000 concurrent SQL
  queries?

  Resource usage shouldn't scale with number of ranges, number of
  tables, number of databases, number of SQL users, or number of
  concurrent SQL queries.  Memory usage usage for a machine hosting
  several clusters should decrease slightly (single-digit MBs), as the
  JS bundle embedded in the `cockroach` binary will be smaller.

### Stability questions

- Can this new functionality affect the stability of a node or the
  entire cluster? How does the behavior of a node or a cluster degrade
  if there is an error in the implementation?

  There should be no changes to a node's overall stability, but it's
  possible for this to impact the JS console's stability.  Since esbuild
  includes its own JS generator and minifier (and not the ones bundled
  with the official TypeScript compiler, the Babel compiler, or
  webpack's "terser" plugin for minification), it's possible that we'll
  be shipping slightly different and less-stable JavaScript. esbuild's
  author has historically been extremely fast to fix code generation
  bugs however, as the correctness of that kind of a system is
  paramount.

  Problems would present as JavaScript errors in an end-user's browser
  console (and indexed in DataDog in the case of
  https://cockroachlabs.cloud users, assuming the error occurs in a
  @cockroachlabs/cluster-ui package), but should have no impact on a
  running cluster itself.

- Can the new functionality be disabled? Can a user opt out? How?

  Because of the source-level changes required, esbuild can't be
  disabled, and end-users can't opt out of using bundles produced by
  esbuild.

- Can the new functionality affect clusters which are not explicitly
  using it?

  N/A: All clusters running instances of vNext (currently v22.1.x) will
  get the new behavior.

- What testing and safe guards are being put in place to
  protect against unexpected problems?

  We'll be relying on automated testing, unit testing, and some manual
  "smoke" testing for the most part.  Because esbuild produces different
  JavaScript than webpack does (different module order, different
  minification patterns, etc.), it's not possible to perform a diff
  between the old and new builds.

### Security questions:

- Does the change concern authentication or authorization logic? If so,
  mention this explicitly tag the relevant security-minded reviewer as
  reviewer to the RFC.

  N/A: No authn or authz changes involved.

- Does the change create a new way to communicate data over the network?
  What rules are in place to ensure that this cannot be used by a
  malicious user to extract confidential data?

  There are minimal changes to how data is communicated over the
  network.  No changes in communication between nodes, but the HTTP
  request pattern for an end-user's page load may change slightly. Since
  these files are entirely static and are consistent across users,
  there's no net-new confidential data extraction mechanisms - just an
  extra request for a .css file, a request for a .js file that goes away
  after this change, etc.

- Is there telemetry or crash reporting? What mechanisms are used to
  ensure no sensitive data is accidentally exposed?

  Crash reporting during a build will be handled by Slack communication
  and GitHub issues filed in the cockroachdb/cockroach repo.  Crash
  reporting at runtime is unchanged.

### Observability and usage questions

- Is the change affecting asynchronous / background subsystems?

  No, only the build process for JS is changing.

- Are there new APIs, or API changes (either internal or external)?

  There are no new APIs.

- Is the change visible to users of CockroachDB or operators who run
  CockroachDB clusters?

  Technically yes - anyone can view the produced JS bundle's source code
  (even if it's minified), and a keen-eyed user or operator may notice a
  difference in JS payload, our JS's styling, etc.  That's not really
  concerning though, as we don't closely control the output format of
  today's webpack build.

  - Are there any user experience (UX) changes needed as a result of
    this RFC?

    Not likely.  We'll need to continue avoiding Flash of Unstyled
    Content (FOUC) issues, but that's not a change from the status quo.

  - Are the UX changes necessary or clearly beneficial? (Cross-reference
    the motivation section.)

    N/A - no changes are expected to be needed.

  - Which principles did you apply to ensure the user experience (UX) is
    consistent with other related features?

    N/A - this should have no significant impact on the presented user
    experience.

  - Which other engineers or teams have you polled for input on the
    proposed UX changes? Which engineers or team may have relevant
    experience to provide feedback on UX?

    N/A - it's not quite a UI change, but I've spoken to some build
    engineers (@rickystewart, @koorosh), CockroachDB devs (@jordanlewis,
    @chrisseto), and JS devs (@laurenbarker, @nathanstilwell) about
    this.

- Is usage of the new feature observable in telemetry? If so, mention
  where in the code telemetry counters or metrics would be added.

  Only by filtering for a version of CockroachDB that includes these
  changes, since they take place purely at build time and have only
  minimal runtime effects.

## Drawbacks

### Zeitgeist & Community

As-of the [2020 State of JS Survey
results](https://2020.stateofjs.com/en-US/technologies/build-tools/#build_tools_experience_marimekko)
(the most recent results available at the time of publishing),
respondents who have used webpack and would use it again outnumber
esbuild users who'd use esbuild again by 5x: 16,234 for webpack vs just
3,382 for esbuild. While there's a fair bit of self-selection in this
survey, it's clear that esbuild hasn't hit the massive adoption that
webpack has.  Moving away from webpack means leaving behind some of that
community support in the form of plugins, blog posts, etc.

While completing [a
prototype](https://github.com/sjbarag/cockroach/commit/f9a17d4dda9ba6c964e87e5fbb817eb6e601c366)
with esbuild as the bundler for CRDB, I haven't found the lack of
community plugins to be a significant impediment.  There's several
competing plugins for similar tasks (e.g. [SASS](sass-lang.com/)
compilation) with no clear "winner" in community adoption, which may
leave us on the "losing" side at a sufficiently long time scale.  This
can also be considered a strategic opportunity though - see
[Opportunities](#opportunities) below.

### Production Readiness

By the [author's own
admission](https://esbuild.github.io/faq/#production-readiness), esbuild
hasn't yet hit "1.0":

> This project has not yet hit version 1.0.0 and is still in active
> development. That said, it is far beyond the alpha stage and is pretty
> stable. I think of it as a late-stage beta. For some early-adopters
> that means it's good enough to use for real things. Some other people
> think this means esbuild isn't ready yet.

Having completed [a
prototype](https://github.com/sjbarag/cockroach/commit/f9a17d4dda9ba6c964e87e5fbb817eb6e601c366)
of an esbuild-based build, I've found the features we need to all be
supported either natively or via community plugins.  Debugging the build
was about as difficult as a webpack-based build, and our use of the
esbuild API is pretty minimal, reducing the risks of a breaking API
change.

Like the [Zeitgeist & Community](#zeitgeist-and-community) drawback
above, CockroachDB adoption of esbuild may be a strategic advantage for
both projects.  See [Opportunities](#opportunities), below.

### Hot Module Reloading

Continuing to use webpack means we'll be able to leverage its native
support for "hot module reloading", which dynamically replaces JS
components without the need for a full-page reload. That functionality
isn't currently being used however (only a "watch" mode that regenerates
the main `.js` file and requires devs to manually reload the page), and
esbuild's watch mode should be significantly faster than webpack's.
Esbuild's maintainer [is
aware](https://github.com/evanw/esbuild/issues/151#issuecomment-652857367)
of the popularity of this feature, and suspects it's possible to support
via a plugin. See [Opportunities](#opportunities), below, yet again.

## Opportunities

Migrating to esbuild for JS bundling can strengthen both CockroachDB
and Cockroach Labs as leaders in the JS space, by providing and
maintaining plugins for the community where appropriate, becoming more
active in various support forums (e.g. Stack Overflow), etc.  Most
esbuild plugins are currently written in JavaScript, but a Go API does
exist.  Our experience writing Go positions us perfectly to advocate for
and influence the direction of esbuild plugins written in Go (most are
written in JavaScript today). There's also an opening for Cockroach Labs
to provide integration between esbuild and React Refresh (a
more advanced implementation of hot module reloading), as we sit in the
intersection between heavy Go use and consistent React use.

It's still very early in the lifecycle of esbuild compared to more
established tools like webpack, and there appears to be space in the
community for Cockroach Labs to take on a leadership role.

## Rationale and Alternatives

### Bundling, generally

Folks who don't spend much time in the JavaScript space (specifically in
the "modern single-page app (SPA)" space) may be asking "Why are we
doing any of this? If webpack is slow, let's just put some `<script>`
tags in an `index.html`, or maybe `cat **/*.js` together."
Realistically, that was the standard for quite a long time and for
smaller applications still is the standard! The modern web development
landscape has gotten quite complicated though &mdash; new versions of
the JavaScript language and standard API are released yearly, with
rapidly evolving browser support &mdash; and writing a large-scale
single-page application requires a bit more intelligence about how those
files are combined. Microsoft's
[TypeScript](https://typescriptlang.org/) language has brought some
much-needed static typing and type-aware tooling to web development, and
when combined with the defacto-standard UI library + state management
pair of [React](https://reactjs.org/) + [Redux](https://redux.js.org/),
a home-grown build system quickly becomes unwieldy.

Despite the frustration it receives, webpack has solved some complicated
problems in the JS space and allows devs to produce size-reduced
applications that can be transferred in a minimal number of HTTP
connections, can be cached and easily updated, etc.

### Current State of Bundlers

With bundling as a requirement for an application of this size and
scope, it's useful to compare the current JS bundling landscape. These
can be split into roughly three categories:

1. (Mostly) Single-threaded bundlers
2. Multi-threaded bundlers
3. Hybrid solutions of 1 and 2

The rest of this section discusses some of those bundlers, but feel free
to skip that. The key takeaway is that
[esbuild](https://esbuild.github.io/) offers the best combination of
build performance, production-readiness, and consistency, with
[snowpack](https://snowpack.dev/) as a possible alternative in the
future as our needs change.

#### Single-threaded Bundlers

Sometimes considered "classic" bundlers, these are typically written in
JavaScript and are inherently single-threaded. Webpack,
[rollup.js](https://www.rollupjs.org/), and [parcel
v1](https://v1.parceljs.org/) are typically listed in this category,
though plugins do exist to parallelize some workloads. Their performance
profiles are pretty similar to each other, and generally have large
unparallelizable steps that place a lower bound on build times.

Since CockroachDB currently uses webpack, we can consider these options
to be the baseline.

#### Multi-threaded Bundlers

Projects like [esbuild](https://esbuild.github.io/) (written in Go) and
[swc](https://swc.rs/) (written in Rust) are the current leaders in
highly parallelized JavaScript tooling. These are growing in popularity
and are significantly faster than existing single-threaded approaches,
but their "newness" often means there's less community support. Both
esbuild and swc offer truly parallel transpilation, but the swc project
doesn't yet offer a stable bundler of its own. The sever-side rendering
framework [Next.JS recently included
SWC](https://nextjs.org/blog/next-12#faster-builds-and-fast-refresh-with-rust-compiler)
in its default configuration to achieve significant speed improvements,
but it still relies on webpack for the final bundling stage. This, again
places a lower-bound of build times.

The similar esbuild project includes a bundling mode that's mature
enough to build the CockroachDB console, meaning webpack is no longer
required. See [drawbacks](#drawbacks) (above) for a discussion on the
production-readiness of esbuild.

#### Hybrid Solutions

Webpack plugins that replace the babel and TypeScript compilers with
esbuild or swc exist, but [initial
prototyping](https://github.com/sjbarag/cockroach/commit/e1e44d06b28d9c0c00e4f295f061fd8745139b5d)
only showed a 1-3 second performance improvement -- mostly due to the
fact that we currently leverage webpack's
[cache-loader](https://v4.webpack.js.org/loaders/cache-loader/) to
short-circuit some compilation. Since most of our time is spent outside
of the compilation step, alternative compilers within our existing
webpack configuration won't introduce significant performance
improvements.

[Parcel v2](https://parceljs.org/) offers a similar approach by-default
here, in which [swc is used by
default](https://parceljs.org/blog/v2/#babel-preset-warnings) for JS
compilation. Surprisingly, [initial
prototyping](https://github.com/sjbarag/cockroach/commit/edf601ab06fe907ab980bd733d1ff10c46356af4)
showed an increase in total compilation time compared to the baseline,
likely due to the plugin architecture used by parcel. (Note that
parcel's automatic plugin discovery means the initial builds are
significantly slower as dependencies are detected, installed, etc.)

Finally, there are tools that bundle only for production builds, and
instead rely on browser support for the newer [ES Modules
format](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Modules)
during development. Both [vite](https://vitejs.dev/) and
[snowpack](https://www.snowpack.dev/) allow for near-instant iteration
without having to recompile and re-bundle an application, and rely on
rollup.js and esbuild (respectively) to produce optimized production
builds. With this knowledge, it's clear that neither vite nor snowpack
can ever produce production builds faster than the standalone bundlers
they depend on. And while their "bundle-less" development models offer
extremely attractive guarantees, the bootstrapping process is
surprisingly slow: for each file used in production (including all
dependencies in `node_modules/`), an ES Module equivalent must be
produced and cached on-disk. This also makes development builds
extremely different from production builds, since different compilers
are used.

Tools like snowpack are useful to track moving forward, but their
reliance on browser-native ES Modules makes failure patterns less clear.
For an infrastructure-level project like CockroachDB, that risk isn't
appropriate yet.

### Impacts of Inaction

There's no stability risks to taking no action here. Webpack builds are
slow, but aren't actively causing production issues. Continuing with
webpack does however mean that developer time and CPU time will be used
inefficiently. Inaction will likely limit the rate at which CockroachDB
features are developed, iterated on, etc.

# Explain it to folk outside of your team

We currently use a project called [webpack](https://webpack.js.org/) to
build our web application, but webpack is pretty slow. It takes a few
minutes for it to build the DB Console in its various formats, and that
time is typically seen as unusable by devs working in CockroachDB.
Switching web app build systems introduces some small risks, but can
save several minutes per build per dev per day.

This change would be transparent to end-users and CockroachDB cluster
operators, as the end-result of this build is embedded in the
`cockroach` binaries they use. This includes:

- No new concepts to introduce to end-users
- No change in thinking for end-users with respect to their applications
  or mental models
- No new error messages or deprecation warnings
- No impact to clusters created before this change (a new binary
  includes the new build system output, so upgrades are atomic)

# Unresolved questions

There's very few unresolved questions at this point. Initial
[protoyping](https://github.com/sjbarag/cockroach/commit/f9a17d4dda9ba6c964e87e5fbb817eb6e601c366)
stopped just short of support for inlined assets (a la
`raw-loader!../path/to/file.ext` in webpack) and hadn't quite worked out
font bundling, but those are significantly smaller concerns.  That
Parcel 2 benchmarking was slower than the existing webpack build is a
bit surprising and needs a bit more investigation. Similarly, snowpack's
maintenance patterns need investigation before it can be considered a
valid option in the future.

Finally, integration with bazel and make need to be confirmed.  Given
that an esbuild-based solution can be triggered from an arbitrary
shell command, this is also of little concern.

# Appendix

## Current Webpack Build Duration
All measurements performed on:
- MacBookPro16,1
- Intel i9-9880H (16-core) @ 2.30GHz
- 32 GB RAM
- macOS 11.6.2
- Commit a4ef73858f (Merge #74172 #74596, 2022-01-12)

### GNU Make
Compare `make buildshort` vs `make build` durations. While crude, it
easily isolates the UI-related steps to show their impact, and webpack
emits duration information by default in our configuration.

```sh
$ make clean && time make -j16 build

# ...

Hash: 5d931f9329dc6efea1d1
Version: webpack 4.46.0
Time: 15638ms
Built at: 01/12/2022 11:28:46 PM
            Asset      Size  Chunks                    Chunk Names
vendor.oss.dll.js  14.8 MiB       0  [emitted]  [big]  vendor
Entrypoint vendor [big] = vendor.oss.dll.js

#...

Hash: 37e8f9487965f89381fd
Version: webpack 4.46.0
Time: 23899ms
Built at: 01/12/2022 11:28:54 PM
            Asset     Size  Chunks             Chunk Names
protos.ccl.dll.js  3.4 MiB       0  [emitted]  protos
Entrypoint protos = protos.ccl.dll.js

# ...

Hash: cb8236ba76e420232498
Version: webpack 4.46.0
Time: 44953ms
Built at: 01/12/2022 11:28:30 PM
      Asset      Size  Chunks                          Chunk Names
    main.js  7.15 MiB       0  [emitted]        [big]  main
main.js.map  27.5 MiB       0  [emitted] [dev]         main
Entrypoint main [big] = main.js main.js.map

# ...

real    4m8.013s
user    24m59.429s
sys     5m10.554s

# ---------------------------------------

$ make clean && time make -j16 buildshort

# ...

real    3m40.873s
user    20m18.886s
sys     4m43.605s
```

The webpack processes took a total of 15638 + 23899 + 44953 ==
84490 ms == 84.49 s, which assumes they were completely serialized and
is therefore an upper-bound on the duration. Comparing `make build` vs
`make buildshort` shows a difference of 28 seconds.

### Bazel
Bazel's built-in profiling shows how much time the existing webpack
steps spent on the critical path:

```
$ bazel build //pkg/cmd/cockroach:cockroach //:go_path --config=with_ui --profile=./clean-build-with-ui.gz
# ...

✔ Cluster-ui: Compiled successfully in 1.36m

# ...

✔ Db-console: Compiled successfully in 47.66s


# ----------------------------------


$ bazel analyze-profile ./clean-build-with-ui.gz
WARNING: Ignoring JAVA_HOME, because it must point to a JDK, not a JRE.
WARNING: This information is intended for consumption by Bazel developers only, and may change at any time. Script against it at your own risk
INFO: Profile created on Wed Jan 12 15:54:55 PST 2022, build ID: bffec989-90bc-4bef-8711-0c2cdbc6a9d8, output base: /private/var/tmp/_bazel_barag/cbbf2fe39c90b0e84b302799936065b2

=== PHASE SUMMARY INFORMATION ===

Total launch phase time         0.120 s    0.02%
Total init phase time           0.015 s    0.00%
Total target pattern evaluation phase time    0.682 s    0.09%
Total interleaved loading-and-analysis phase time   33.664 s    4.66%
Total preparation phase time    0.024 s    0.00%
Total execution phase time    687.364 s   95.22%
Total finish phase time         0.011 s    0.00%
------------------------------------------------
Total run time                721.882 s  100.00%

Critical path (369.605 s):
       Time Percentage   Description
     309 ms    0.08%   action 'Executing genrule @com_google_protobuf// _internal_wkt_protos_genrule'
    3.65 ms    0.00%   action 'Symlinking virtual .proto sources for @com_google_protobuf// timestamp_proto'
   20.326 s    5.50%   action 'Action pkg/ui/workspaces/db-console/src/js/protos.js'
   59.958 s   16.22%   action 'Action pkg/ui/workspaces/db-console/src/js/protos.d.ts'
  176.389 s   47.72%   action 'Action pkg/ui/workspaces/cluster-ui/dist/js/main.js'
   82.787 s   22.40%   action 'Action pkg/ui/workspaces/db-console/db-console-ccl'
     803 ms    0.22%   action 'Executing genrule //pkg/ui/distccl genassets'
     233 ms    0.06%   action 'GoCompilePkg pkg/ui/distccl/distccl.a'
     398 ms    0.11%   action 'GoCompilePkg pkg/cmd/cockroach/cockroach.a'
   28.398 s    7.68%   action 'GoLink pkg/cmd/cockroach/cockroach_/cockroach'
    0.08 ms    0.00%   runfiles for //pkg/cmd/cockroach cockroach
```

Bazel builds appear to be slower. Webpack reports a build time of
roughly 81 seconds for cluster-ui and 47 seconds for db-console, and
bazel reports critical-path times of 176 seconds for cluster-ui and 82
seconds for db-console.
