# Cluster UI Components

This library contains components used by the CockroachDB Console
and CockroachCloud to display cluster-level information.

```shell
$ npm install --save-dev @cockroachlabs/cluster-ui
```

Components are exported individually from the package,

```javascript
import { Drawer } from "@cockroachlabs/cluster-ui";

export default props => (
  <div>
    <Drawer />
  </div>
);
```
# Overview of how this package interacts with CRDB and CockroachCloud

The diagram below describes how this package is published
and imported in the two application codebases it's used in:

![@startuml
title "Javascript Dependency Diagram"
package "DB Console (cockroachdb/cockroach/pkg/ui)" {
[DB Console App]
[protos-js]
}
note bottom of [protos-js] : Client and types\ngenerated from\nprotos in CRDB
package "Cluster UI (cockroachdb/ui/packages/cluster-ui)" {
[cluster-ui code]
}
frame "cockroachlabs/managed-service" {
[CC Frontend App]
}
cloud "NPM Javascript Packages" {
[crdb-protobuf-client]
[cluster-ui]
}
[protos-js] -> [crdb-protobuf-client] : Manually published
[cluster-ui code] --> [cluster-ui]: Automatically published\non merge to master
[cluster-ui code] <-- [crdb-protobuf-client]
[CC Frontend App] <-- [cluster-ui]
[DB Console App] <-- [cluster-ui]
@enduml](http://www.plantuml.com/plantuml/png/TPB1Qjj048RlUef1f_Qmw6vAI29JQ24b57ggVHZjZfMjTsUMtTc0ANttMew99HpVnEZ_Hjy_wBuePgqnDEer4BJVyHMBpJufh2aHEs9xWBN7CMDicuHsZ-Cnjtw4NhZ8aVbanUwpe7rnG_V-tANzs5N_kOM_3S3lMVuXfUqqIbbKYlbJjis_XaK91b6L2BARluGLzC4JAo0xq4EYik6Hc38gETXbYHj-YuDdw7-k7vkBPnzgKShmwzlIi_hkd2cTVSkOY-rb0bOSJOBDBcCaQD-N11nA5v5n96SAvLTlwOptFNpDmahifhOJReDK1-sFvoUOdVZvh73cR7Q3ELKPwixOK-ljqkUaCh-EkRl1mGgUa2k6S81KX-3B2xdcgXeOSxVum0eUgaf4zNR9RbWO8kMHG0KYJi07-xuOSwl9rM6cyBBTx4UvaRW_mZM6_m00)

To look one level of abstraction below the diagram above, the
components used in CockroachCloud and CRDB from this package differ
due to the way that the component state management code integrates
with the different apps. In CRDB, we import the React page component
and connect it to _existing Redux code that manages state_ whereas
in CockroachCloud, we import a "Connected component" which is already
coupled to the Redux code that retrieves and manages its data. In
CockroachCloud we link the `cluster-ui` redux store to the global
app store which lets our Connected component store and retrieve data.

We are likely to move the Statements Page Redux/Saga
code below directly into CockroachCloud's repository
to clarify the fact that each app manages its own
state-related code and that none of that is shared.

![@startuml
left to right direction
title "High-level component architecture for Cluster UI\n(Statements Page example)"
package "DB Console (cockroachdb/cockroach/pkg/ui)" {
[CRDB Redux]
[Statements Page in CRDB] <<unconnected>>
[protobuf client]
note right of [protobuf client] : local version
note right of [CRDB Redux]: global app state
}
package "Cluster UI (cockroachdb/ui/packages/cluster-ui)" {
component "Connected Statements Page" <<connected>> {
[Statements Page] <<unconnected>>
[Statements Page Redux/Saga]
}
[crdb-protobuf-client]
note right of [crdb-protobuf-client] : npm version
note right of [Statements Page Redux/Saga]: rewritten for extraction to work\nwith CC specifically
}
frame "cockroachlabs/managed-service" {
[CC Redux]
[Statements Page in CC] <<connected>>
note right of [CC Redux]: global app state
}
skinparam component {
backgroundColor<<unconnected>> DarkKhaki
backgroundColor<<connected>> RosyBrown
}
[Statements Page in CRDB] -> [CRDB Redux]: Uses existing redux code\npresent prior to extraction
[CRDB Redux] -> [protobuf client]: Uses types and API client
[Statements Page Redux/Saga] -> [crdb-protobuf-client]: Uses types and API client
[Statements Page] -> [crdb-protobuf-client]: Uses types
[Statements Page in CC] -> [CC Redux]: Merges its redux\nstate tree with app
@enduml](http://www.plantuml.com/plantuml/png/dPD1Jzj048Nl_XKZd-0G-OuGefX3KLK9WJWr73RhiRFoUcOrEoP15V-zZ4AeQn9QzPQitypi--RjPLOdEWwnYDWgA8E4RgtG146lWQdGe16X_Ffwl8ZuX14y3ua9IS69NmT5hwCWj2nGnp4h2ZpSBEdaNftYWAGPRbs7W5itf8YdPL4avtixAg-le6RA715EFFjUsFbriwhUVrUfxwend9Rmim3uKT-zLjnXCsuVxUzyH20mnIESdu_acGYzOdDnOTeahBmQM_0n8AdL4ok-6URsemBE8B9t4PvGih6OLqolSmPTv9MBu5A2RFSgNeg9qzze_dGwXkfDaok_qopsVYUeothl1cQcorUp4wjM1n_HV41oKBNpKjsxpXcV5-FcfLajzcWMH-0TbXb1IiDnVf-CFmF1ZGHL98iMu5R5MIJ9KhfXwPUq2Rg6keQSq8SsU1VZir5lnGq8vJlcw5Qv6Xov3fj5HdaA7lU1glyIfzh8JRaTI47zQGPo7oWvSSDadJPxvNpV2O_Kr1nPPlk1QoVzjxNhmo7fL7Z7-VbAU4CsxFYJM5pCylgGCGDkGzP07OYTWkS6bvG4izqoIM01vGdyOjh3UivVpRwnFYVCuAY1BxStRz-AuhEDMxTvZFwZvJ_sEOAgtfLFLleTfSCCGVEEqPAorO4A8bW2RTl59L8p3l4N)


# Development Workflow with CRDB

Make sure you have a `node` version that is below `15`. Versions
above `14` currently cause issues with compiling `node-sass`. This
is a dependency we will hopefully replace at some point to eliminate
this blocker. You can either use `brew` or your local package
manager to install a specific node version or a tool like `nodenv`
which lets you manage multiple locally installed `node` versions.

If you would like changes in this package to be reflected
in a locally running DB Console instance, you will need to
locally link the library to your DB Console project like this:

In `packages/cluster-ui` run:

```shell
$ yarn
$ yarn link
$ yarn run build:watch
```

Then in `pkg/ui` in CRDB repo run:

```shell
$ yarn link "@cockroachlabs/cluster-ui"
$ cd ../../
$ make ui-watch TARGET=http://localhost:8080
```

Now open up `http://localhost:3000` to see the development
DB Console which will auto-refresh when you make any changes.

We also recommend installing the React Devtools
and Redux Devtools to help you in your workflow.

While running a local CRDB cluster. This will link your DB Console
code to the local copy of `cluster-ui` and the `build:watch` task
in `cluster-ui` will update the build with any changes you make
which will get picked up in the `ui-watch` job on the DB side.

## PR Workflow with Protobuf Changes

The process for making a change that also relies on db-level
protobuf changes will look something like this. See the
section below for publishing CRDB Protobuf definition updates.

![@startuml
title "Development process with CRDB when using new protos"
"CRDB Repo" ->o "CRDB Repo": PR in CRDB with new protos
"CRDB Repo" -> "NPM": Publish new proto client\n(manual process)
"NPM" -> "UI Repo": Update version\nin package.json\nto use new client
"UI Repo" ->o "UI Repo": PR in UI repo with new changes
"UI Repo" -> "NPM": Version is bumped\nand published automatically
"NPM" ->o "CRDB Repo": PR in CRDB to update\ncluster-ui version\nin package.json
@enduml](http://www.plantuml.com/plantuml/png/VP2nJiD038RtF8ML2ORo00oeWYuCe2fIcRAu5ointFbEiQyHRqzpKPK82SO_yMT__-tLSBGSV6Lidg0-q8LyJ87488tHaIfCR0EyD8Tdc0OIoChIWz0q3rZKkghBpuPIh67t566J7-7O0Ck2bqKh-8k3-ltuDWFvx5atW-0yarWhTm4bex-9tLU5AEZfzNRlb3eqWWkDob5QOO64xWjxUlZK-OD5o4fb_RAuAlHglwJL_Ph7QrxrtO3IaswurVvZkGkiSCuXKTSAIWTfAKKTOBOOqDYXzz-bmV-FDkkMIgqudzLet6N-irwr9-boy3y0)

# Development Workflow with CockroachCloud

_Note: The above workflow won't work with CockroachCloud
due to the fact that `yarn link` works poorly with
peer dependencies, which redux is for this library._

In `packages/cluster-ui` run:

```shell
$ yarn build
$ yarn pack --prod
```

And then install the package in `managed-service` repo:

```shell
$ cd /console
$ yarn cache clean
$ yarn add --force /Users/<full path to cluster-ui>/cockroachlabs-cluster-ui-vXXXX.tgz
```

# Storybook

Learn more about Storybook here: https://storybook.js.org/

You can run storybook for this project using:

```shell
$ yarn run storybook
```

And opening http://localhost:6006/ in your browser.

The use of storybook in this project is to provide
a controlled environment where components can be
rendered or showcased for testing and sharing purposes
outside of the entire DB Console or CockroachCloud app.

Often we build a component that behaves or looks differently in
different scenarios with different inputs. Storybook lets us
render the component in all the "interesting configurations" we
define so that we can quickly get visual feedback on our work
and ensure that we're not breaking functionality when editing
code. Otherwise we waste lots of time getting our app in the
"right state" to try and see if some random feature works or not.

Components and page developers are encouraged to use Storybook to
showcase components and pages in many common states to be able to
quickly review their  work visually and to aid others in understanding
what the various states pages and components we want to support.

Storybook stories **should** use the CSF format as described
here: https://storybook.js.org/docs/react/api/csf in order to
facilitate writing unit tests with the storybook components.

# Updating Protobuf definitions from CRDB

The `cluster-ui` components rely on protobuf types and code from
CRDB in order to know what data shapes they should expect and how
to query them. Sometimes you might need to develop components using
updated definitions that were merged recently or are in progress.

The protobuf definitions are currently manually
published from the CRDB repo into this package:
https://www.npmjs.com/package/@cockroachlabs/crdb-protobuf-client

### How to publish protobuf client updates to npm

Once you have updated definitions locally, run `make ui-lint`
to generate them and make sure typescript is run, then modify
the `pkg/ui/src/js/package.json` file with a new version (if
your work isn't merged yet, use `0.0.<next>-beta.<integer>`)
like `0.0.23-beta.0` if the latest one is `0.0.22` and run:

```shell
$ npm publish --access public
```

This will push a new version to npm and you can bump
the dependency in `pkg/ui` in this repo to use it.

If you'd like permission to do this, please ask in #_cc-observability
on Slack. Otherwise, Observability team is happy to publish a

We're hoping to move this into an automated workflow to publish
on all merges to `master` on CRDB to remove this pain point.

### Expert Level

You can use the same `yarn link` workflow described above to
link the `crdb-protobuf-client` to `cluster-ui` locally if
you'd like to live on the fully bleeding edge :sunglasses:

