# Cluster UI Components

This library contains components used by the CockroachDB Console
and CockroachCloud Console to display cluster-level information.

## Overview of how this package interacts with CRDB and CockroachCloud

The diagram below describes how this package is published
and imported in the two application codebases it's used in:

![@startuml
title "Cluster UI, Dependency Diagram"
folder "cockroachdb/cockroach" {
  frame "master" {
    node "pkg/ui" as mui
    node "pkg/ui/cluster" as mcui
  }
  frame "release-21.1" {
    node "pkg/ui" as 211UI
    node "pkg/ui/cluster-ui" as 211cui
  }
  frame "release-20.2" {
    node "pkg/ui" as 200UI
    node "pkg/ui/cluster-ui" as 200cui
  }
}
cloud "NPM" {
  frame "@cockroachlabs/cluster-ui" {
    [21.2.x]
    [21.1.x]
    [20.2.x]
  }
}
folder "cockroachlabs/managed-service/console" {
  [@cockroachlabs/cluster-ui-21-2]
  [@cockroachlabs/cluster-ui-21-1]
  [@cockroachlabs/cluster-ui-20-2]
}
[mui] <-l-- [mcui]
[211UI] <-l-- [211cui]
[200UI] <-l-- [200cui]
[mcui] --d-> [21.2.x]
[211cui] --d-> [21.1.x]
[200cui] --d-> [20.2.x]
[21.2.x] --d->[@cockroachlabs/cluster-ui-21-2]
[21.1.x] --d-> [@cockroachlabs/cluster-ui-21-1]
[20.2.x] --d-> [@cockroachlabs/cluster-ui-20-2]
@enduml](http://www.plantuml.com/plantuml/png/XPCnRy8m48Nt-nMdp3KucQiAgImChRemHGp6FcY4ao0xgLfL_FSwN8s5KEIDU_VyFV5EMVb1kM5iBGpDO0cBLplwWHnkDq-ufZDrXZhzW-j67Prg2u13RqtO5xhN9zSh_Mdsozll0dy1yH2S0TMgYSGIOjURe9rFn-NO5AWyjcFpi5XgZcU3lZekYUZ8al8agd9HpdAhijnkS1OjacsUBnVLF5_AxIQFbpBYBm3QzgF1ultZxQwXrQqug_R-3i7XTVYdrU9xTnlAD4ZUSCB3MPZOgauToGXFxglH50xL-TuIu-lP-52mg7PPIvcpo8bo0QZ3hNVuBAmGMBSw351VpnJ5QVgNfKpoD5rbu5SeX14liHM693qMn9IafwuWlkH5je08P7k-ZHYKztCriABEJ1yVuXy0)

To look one level of abstraction below the diagram above, the
components used in CockroachCloud and CRDB from this package differ
due to the way that the component state management code integrates
with the different applications. In CRDB, we import the React page component
and connect it to _existing Redux code that manages state_ whereas
in CockroachCloud, we import a "Connected component" which is already
coupled to the Redux code that retrieves and manages its data. In
CockroachCloud we link the `cluster-ui` redux store to the global
app store which lets our Connected component store and retrieve data.

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

## Version alignment with CRDB releases

`cluster-ui` is dependent on the API of CRDB through a dependency on protobuf clients.
Since the API can change in incompatible ways between major versions of the database,
versions of cluster-ui are published following the version scheme of CockroachDB.

| CRDB Version | Cluster UI Version | development branch |
|--------------|--------------------|--------------------|
| v20.2.x      | 20.2.x             | release-20.2       |
| v21.1.x      | 21.1.x             | release-21.1       |
| v21.2.x      | 21.2.x             | master             |

Note, that the patch versions of the CRDB version and the Cluster UI version will
not align. The versioning schemes between CRDB and Cluster UI may vary slightly,
but the goal is to provide guidance about compatibility.

## Development Workflow with CRDB

The build processes for Cluster UI have been integrated into the `make` commands
for `/pkg/ui`. The intention is that the development workflow between DB Console
and Cluster UI components be seamless. If you would like to build cluster-ui
independently, you can run the following commands,

```shell
  > make pkg/ui/yarn.protobuf.installed -B; # to build protobufs cluster-ui depends on
  > make pkg/ui/yarn.cluster-ui.installed -B; # to build cluster-ui
```

## Development Workflow with CockroachCloud

In CockroachCloud Console, we install multiple *aliased* versions of cluster-ui.
This, in addition to certain peer dependencies, prevent us from using a traditional
`yarn link` for local development of cluster-ui. Instead, you must *pack* the dependency
and install it from a local tarball. To view a local change to cluster-ui in CockroachCloud,

```shell
# after making your code change,
# in the root of cockroachdb/cockroach
$ make pkg/ui/yarn.protobuf.installed -B
$ make pkg/ui/yarn.cluster-ui.installed -B
# in pkg/ui/cluster-ui
$ yarn run build
$ yarn pack --prod
```

And then install the package in `managed-service` repo:

```shell
$ cd /console
$ yarn cache clean
# yarn add --force [cluster-ui package alias]@[full path to tarball],
# here's an example
$ yarn add --force @cockroachlabs/cluster-ui-21-1@/path/to/cockroachlabs-cluster-ui.tgz
```

Test your changes by running CockroachCloud locally, but please be careful to not
commit the changes to `package.json` or to `yarn.lock`

## Storybook

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

## Publishing Cluster UI package to npm
WIP

### 1. Change the version in package.json
CockroachDB uses a form of [calver](https://calver.org/) versioning where the major part
is a *Short Year* and the minor part is an iterative number.The micro part (in
semver, this is called the "patch" part) is also an iterative number. The first
two numbers comprise a major version of CockroachDB, and the third denotes a minor
release of that major version. For example, `21.1.6` is a version released in 2021,
is part of the first major release of that year, and the seventh release in that
series (`21.1.0` being the first).

npm package versions must be parseable by [semantic versioning](https://docs.npmjs.com/cli/v6/using-npm/semver). To denote what versions of the cluster-ui package are intended for use with specific major version of CockroachDB, cluster-ui mimicks the version of CockroachDB. For example `cluster-ui@21.1.4` is a version of cluster-ui compatible with the API of CockroachDB version `21.1` and is the fourth release in this major version. It's important to note that `cluster-ui@21.1.4` is **not** directly published from CockroachDB version `21.1.4`. Only that `cluster-ui@21.1.x` was published from CockroachDB branch `release-21.1`.

So when incrementing (or "bumping") the version of cluster-ui the only number that should change will be the "patch"
version (the third number). For example, if you're working on master and find the version to be `21.2.1` you would
change the version to be `21.2.2`. The version change should accompany the pull-request with the changes to the code, and should be merged with them.

### 2. Create a git tag

Once a pull-request has been merged into respective branch, a git tag should be made against the commit that contains
the changes to cluster-ui and the version change. The form of the tag should be `@cockroachlabs/cluster@[version]`
where `[version]` is the version found in the `package.json`. After creating a tag, push the tag to a remote referencing
`git@github.com/cockroachdb/cockroach.git`.

```shell
$ git tag [tag name] [SHA] # SHA can be omitted if desired commit is HEAD
$ git push [remote] [tag name]
```

### Build Cluster UI prerequisites (protobufs, etc)
To build the prerequisites for cluster-ui run the following commands from the root
directory of CockroachDB.

```shell
$ make ui-maintainer-clean
$ make pkg/ui/yarn.protobuf.installed -B
$ make pkg/ui/yarn.cluster-ui.installed -B
```

### 4. Publish
If you have not done so already, request publishing permissions from the Cluster
UI team at Cockroach Labs. You will be required to create an account on [npm](https://www.npmjs.com/)
and configure two factor authentication. You will then be added to the `@cockroachlabs`
npm organization and given permission to publish the `clutser-ui` package.

Before publishing, ensure that you have authenticated with the npm registry by
running the following command,

```shell
$ yarn login
```

To log in, you will be prompted for your credentials.
```shell
$ yarn login
yarn login v1.22.10
question npm username: *********
question npm email: **************@*****.***
✨  Done in 11.56s.
```

If you were logged in already, you will see the current authenticated user
```shell
$ yarn login
yarn login v1.XX.XX
info npm username: xxxxxxxxxxx
info npm email: xxxxxxxx@xxxxx.zzz
✨  Done in 0.05s.
```

Once you have authenticated, you will run the publish command like so

```shell
$ yarn publish --access public
```

During a publish you can expect to see some prepublish steps before the package
is delivered to the registry. `clean` will delete the contents of the `dist`
directory and `build` will compile TypeScript and bundle the code using Webpack.
The publish will create a tarball and send it to the registry.

### 5. Add a distribution tag
We use [distribution tags](https://docs.npmjs.com/adding-dist-tags-to-packages) to organize the versions of `cluster-ui`. By default
the most recent published version will be tagged as `latest`. Depending on the
version you are publishing, you will most likely need to add distribution tags to
the version you published. Our current distribution tags are,

dist tag name| example version | purpose
-------------|--------
latest       | 21.1.2 | latest version of major stable version of CRDB
release-21.1 | 21.1.2 | latest version from version 21.1 of CRDB
release-20.2 | 20.2.3 | latest version from version 20.2 of CRDB
next         | 21.2.0-prerelease-1 | latest version of development version of CRDB

At the time of writing, CockroachDB v21.1 is the latest stable version so cluster
ui versions from this release branch should be tagged as both `latest` and `release-20.1`.
CockroachDB v21.2 is currently under development and so versions published from
master should be tagged as `next` and are expected to have a [prerelease identifiers](https://semver.org/#backusnaur-form-grammar-for-valid-semver-versions)
(a portion of the version appended by a `-`, for example `21.2.0-prerelease-1`)
This is to designate that a `.0` version of `21.2` should be reserved until CockroachDB v21.2
is released publicly.

Distribution tags are easily modified and so there should be little concern about
adding or removing them as needed. To add (or modify) a distribution tag, run the
following command,

```shell
$ npm dist-tag add @cockroachlabs/cluster-ui@[version] [dist tag name];
# for example,
# npm dist-tag add @cockroachlabs/cluster-ui@21.1.2 release-21.1
# npm dist-tag add @cockroachlabs/cluster-ui@21.2.0-prerelease-2 next
# npm dist-tag add @cockroachlabs/cluster-ui@21.1.0 latest
```

If a mistake is made, simply add again the correct version/tag pair or remove the
tag with the following command,
```shell
$ npm dist-tag rm @cockroachlabs/cluster-ui [dist tag name];
```

To see what the current distribution tags for cluster-ui, run
```shell
$ npm dist-tag ls @cockroachlabs/cluster-ui
# example output:
# latest: 21.1.1
# next: 21.2.0-prerelease-1
# release-20.2: 20.2.0
# release-21.1: 21.1.1
```
