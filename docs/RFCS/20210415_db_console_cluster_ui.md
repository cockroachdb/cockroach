- Feature Name: Reimport Published Cluster UI into DB Console
- Status: draft
- Start Date: 2021-04-15
- Authors: Nathan Stilwell
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

**Remember, you can submit a PR with your RFC before the text is
complete. Refer to the [README](README.md#rfc-process) for details.**

**Remember, you can either fill in this template from scratch, for
example if you prefer working from a blank slate, or you can follow
the writing prompts in the [GUIDE](GUIDE.md). In any case, please ensure
at the end that you have all relevant topics from the guide covered in
your prose.**

# Summary

After a year of extracting "pages" from the DB Console, we are proposing we _put them back_ into the CRDB codebase in `/pkg/ui`. While the extraction project has been ultimately successful, in that the page components were refactored to function in CockroachCloud, developing these components for DB Console has become more arduous. With the diverging contexts of DB Console and CockroachCloud, it is not suitable to develop these single components for use in multiple applications and so ultimately we want to have two distinct user interfaces in these applications based on the single API of CRDB.

This is not to say that the user interface features of a cluster don't have common elements between DB Console and CockroachCloud Console. Where the UI is consistent I am still proposing development of common ui components to function in both applications. I believe that what is common between the applications is the presentation UI (generic elements for input, notification, and display) rather than the application layer (routing, application state, and fetching data). The code that will be copied back into the `cockroachdb/cockroach` repository from the `cockroachdb/ui` repository will the application specific components which should cause minimal disruption. Most of the adaptation of these components was for the purpose of importing them into CockroachCloud Console. The challange of continuing to develop in that repository and application are outside of the scope of this RFC. The one remaining connection between the two applications will be continuing to publish `@cockroachlabs/crdb-protobuf-clients` as components running in CC Console will continue to use the API from a CRDB cluster. 

The impact of this change that I am hoping for is to simplify development and maintenance of DB Console. As ownership of parts of this application will fall to teams primarily working in this repository, it is important to support development there as much as possible. To that end, part of the advantage of extracting portions of the DB Console application was to simply development and testing.

# Motivation

The components that are rendered for `/statements`, `/transactions`, and `/sessions` were extracted (the code copied and then refactored) from DB Console into another repo intended to publish UI dependencies. The motivation here was that they would be consumed by CockroachCloud console as a dependency. The _presentational_ components themselves had depenencies on may other presentational components (_list dependencies here_) that also needed to be copied. These components also dependend on _connected_ components to take relevant data from the application state and pass them as props to the component. These componets are generally lists of things that _link_ to another component to display an individual item (Statements -> Statement Details, Transactions -> Tranaction Details, etc) and so also depended on the ability to use the applications routing. All of these mechanisms were established in DB-Console, but are incompatible with the CockroachCloud console application. So code had to be created to integrate these features into the application state and routes of CC Console.

As it stands today most of the code that is published in `@cockroachlabs/cluster-ui` (the package containing these components) is to support the features of CockroachCloud Console. In an effort to keep these features consistent between DB Console and CC Console, the presentational components were replaced in DB Console leaving in place the routing and state managment. CRDB still serves as the API for the application state that supports these presentational components which is still housed in CRDB (`/pkg/ui/src/js/protos.js`). This creates a crisscross of dependencies that make local development increasingly challenging.

![cluster ui dependency in db console](http://www.plantuml.com/plantuml/png/VP31IWCn443l-OfXJte8-G6HQhqeABruAdl8JiOQDys4P2A8-E-sTZMxeDwIuStZCQiYDalT1oLU0a6t3hK8PNYy1KU9egE8R-0Zt5p3ccFXG9rA5aUxEz1j26V8E6Qs-Em6y_CsQToPwiyxU5VR6NsLKg-sARDmqI-bLnzDsXAMqkhDn1I39qP_gziVa1qTVHYuEkWUDlTmPrzjmUoJm6OodBQo6_HNn52VD0PFKVgvxE2PLuA-XF-NMW7e8zoclo4NMl_fMnvhOkUw5KLNz-4J)

For local development, dependency management tools can create symlinks for dependency development. Developing with a single dependency, and thus a single symlink, is fairly trivial and a common practice among developers of published npm packages. Adding layers of dependency, and thus symlinks, increases the difficulty and combining layers of dependency and crisscrossing repos increases it further. This is due to the nature of frontend development tools needing to "watch and rebuild" these symlinked dependencies and so the more symlinks your are managing, the more build processes you are juggling simultanously.

This level of effort was considered a cost of maintaining feature parity across applications, when those features needed to be identical. Over time we have seen the need to diverge these features (for example presenting a view of Statements slightly differently in CockroachCloud than in DB Console) calling into question the value of this architecture. Upon reflection, I feel this is correct as I imagine we will want to present a different user experience for a developer or operator in a Serverless or Free Tier CockroachCloud context than an on-prem one in DB Console.

This leads me to how we might simplify this situation. Since the only code we are using from `@cockroachlabs/cluster-ui` are the presentational components, by copying the refactored components back into the CRDB codebase there will be no symlinks required to develop features there.

![no symlinks](http://www.plantuml.com/plantuml/png/TO-noi8m58NtFCKbq_yFeMyWrReeA5sSIeVq9jROc8JaHX3ntQqLj8lRvVATS_ZAH39IlbX6Xgm6NjcSI4SuR43fe9tI3czW7AnNK7cNlDMS1Oc3x4Vw-uG_z3X4B_6akk9p5s7eYsp-ETnPCwEN8RIi2T6JJ-ASMlPWQ7rMmFVr3hRoAXD64mZgzrIQ2Z2gJaoXBXy5MgCTlQAp8X0B-C6Leyre-WG0)

# Technical design

WIP

_still working on this section_

- copy `StatementsPage`
- copy `SessionDetails` and `SessionsPage`
- copy `TransactionsPage`

There are 30 something other occurances of `@cockroachlabs/cluster-ui` in CRDB, so we will need to account for other components that were copied out to support these components by copying them back into the codebase, moving them to `@cockroachlabs/ui-components` and importing from there, replacing them with existing components from `ui-components` or some other refactoring. 

```
34 results - 32 files

pkg/ui/ccl/src/views/clusterviz/containers/map/breadcrumbs.tsx:
  14  import { LocalityTier } from "src/redux/localities";
  15: import { util } from "@cockroachlabs/cluster-ui";
  16  import { getLocalityLabel } from "src/util/localities";

pkg/ui/ccl/src/views/clusterviz/containers/map/index.tsx:
  19  import { parseLocalityRoute } from "src/util/localities";
  20: import { Loading } from "@cockroachlabs/cluster-ui";
  21  import { AdminUIState } from "src/redux/state";

pkg/ui/ccl/src/views/clusterviz/containers/map/nodeCanvasContainer.tsx:
  42  import { getLocality } from "src/util/localities";
  43: import { Loading } from "@cockroachlabs/cluster-ui";
  44  import { NodeCanvas } from "./nodeCanvas";

pkg/ui/src/app.spec.tsx:
  34  import { DataDistributionPage } from "src/views/cluster/containers/dataDistribution";
  35: import { StatementsPage, StatementDetails } from "@cockroachlabs/cluster-ui";
  36  import Debug from "src/views/reports/containers/debug";

pkg/ui/src/views/cluster/containers/dataDistribution/index.tsx:
  17  
  18: import { Loading } from "@cockroachlabs/cluster-ui";
  19  import { ToolTipWrapper } from "src/views/shared/components/toolTip";

pkg/ui/src/views/cluster/containers/events/index.tsx:
  31  import { ToolTipWrapper } from "src/views/shared/components/toolTip";
  32: import { Loading } from "@cockroachlabs/cluster-ui";
  33  import "./events.styl";

pkg/ui/src/views/cluster/containers/nodeLogs/index.tsx:
  27  import { getDisplayName } from "src/redux/nodes";
  28: import { Loading } from "@cockroachlabs/cluster-ui";
  29  import { getMatchParamByName } from "src/util/query";

pkg/ui/src/views/cluster/containers/nodeOverview/index.tsx:
  35  } from "src/views/shared/components/summaryBar";
  36: import { Button } from "@cockroachlabs/cluster-ui";
  37  import { ArrowLeft } from "@cockroachlabs/icons";

pkg/ui/src/views/cluster/containers/nodesOverview/index.tsx:
  31  import { Text, TextTypes, Tooltip, Badge, BadgeProps } from "src/components";
  32: import { ColumnsConfig, Table } from "@cockroachlabs/cluster-ui";
  33  import { Percentage } from "src/util/format";

pkg/ui/src/views/databases/containers/databases/nonTableSummary.spec.tsx:
  20  import { cockroach } from "src/js/protos";
  21: import { Loading } from "@cockroachlabs/cluster-ui";
  22  import NonTableStatsResponse = cockroach.server.serverpb.NonTableStatsResponse;

pkg/ui/src/views/databases/containers/databases/nonTableSummary.tsx:
  17  import { Bytes } from "src/util/format";
  18: import { Loading } from "@cockroachlabs/cluster-ui";
  19  import { CachedDataReducerState } from "src/redux/cachedDataReducer";

pkg/ui/src/views/databases/containers/tableDetails/index.tsx:
  32  import { getMatchParamByName } from "src/util/query";
  33: import { Button } from "@cockroachlabs/cluster-ui";
  34  import { ArrowLeft } from "@cockroachlabs/icons";

pkg/ui/src/views/jobs/index.tsx:
  22  import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
  23: import { Loading } from "@cockroachlabs/cluster-ui";
  24  import {

pkg/ui/src/views/jobs/jobDetails.tsx:
  26  import { showSetting, statusSetting, typeSetting } from ".";
  27: import { Loading } from "@cockroachlabs/cluster-ui";
  28  import SqlBox from "../shared/components/sql/box";

  33  import JobsResponse = cockroach.server.serverpb.JobsResponse;
  34: import { Button } from "@cockroachlabs/cluster-ui";
  35  import { ArrowLeft } from "@cockroachlabs/icons";

pkg/ui/src/views/jobs/jobTable.tsx:
  25  import JobsResponse = cockroach.server.serverpb.JobsResponse;
  26: import { Pagination, ResultsPerPageLabel } from "@cockroachlabs/cluster-ui";
  27  import { jobTable } from "src/util/docs";
  28  import { trackDocsLink } from "src/util/analytics";
  29: import { EmptyTable } from "@cockroachlabs/cluster-ui";
  30  import { Anchor } from "src/components";

pkg/ui/src/views/reports/containers/certificates/index.tsx:
  24  import { LongToMoment } from "src/util/convert";
  25: import { Loading } from "@cockroachlabs/cluster-ui";
  26  import { getMatchParamByName } from "src/util/query";

pkg/ui/src/views/reports/containers/localities/index.tsx:
  31  import { findMostSpecificLocation, hasLocation } from "src/util/locations";
  32: import { Loading } from "@cockroachlabs/cluster-ui";
  33  import "./localities.styl";

pkg/ui/src/views/reports/containers/network/index.tsx:
  37  } from "src/views/reports/components/nodeFilterList";
  38: import { Loading } from "@cockroachlabs/cluster-ui";
  39  import { Latency } from "./latency";

pkg/ui/src/views/reports/containers/nodeHistory/decommissionedNodeHistory.tsx:
  27  import { Text } from "src/components";
  28: import { ColumnsConfig, Table } from "@cockroachlabs/cluster-ui";
  29  import { createSelector } from "reselect";

pkg/ui/src/views/reports/containers/problemRanges/index.tsx:
  26  import ConnectionsTable from "src/views/reports/containers/problemRanges/connectionsTable";
  27: import { Loading } from "@cockroachlabs/cluster-ui";
  28  import { getMatchParamByName } from "src/util/query";

pkg/ui/src/views/reports/containers/range/allocator.tsx:
  17  import Print from "src/views/reports/containers/range/print";
  18: import { Loading } from "@cockroachlabs/cluster-ui";
  19  

pkg/ui/src/views/reports/containers/range/connectionsTable.tsx:
  16  import { CachedDataReducerState } from "src/redux/cachedDataReducer";
  17: import { Loading } from "@cockroachlabs/cluster-ui";
  18  

pkg/ui/src/views/reports/containers/range/logTable.tsx:
  17  import Print from "src/views/reports/containers/range/print";
  18: import { Loading } from "@cockroachlabs/cluster-ui";
  19  import { TimestampToMoment } from "src/util/convert";

pkg/ui/src/views/reports/containers/settings/index.tsx:
  19  import { AdminUIState } from "src/redux/state";
  20: import { Loading } from "@cockroachlabs/cluster-ui";
  21  import "./index.styl";

pkg/ui/src/views/reports/containers/statementDiagnosticsHistory/index.tsx:
  51    getDiagnosticsStatus,
  52: } from "@cockroachlabs/cluster-ui";
  53  

pkg/ui/src/views/reports/containers/stores/index.tsx:
  22  import EncryptionStatus from "src/views/reports/containers/stores/encryption";
  23: import { Loading } from "@cockroachlabs/cluster-ui";
  24  import { getMatchParamByName } from "src/util/query";

pkg/ui/src/views/sessions/sessionDetails.tsx:
  26  import { nodeDisplayNameByIDSelector } from "src/redux/nodes";
  27: import { SessionDetails, byteArrayToUuid } from "@cockroachlabs/cluster-ui";
  28  import {

pkg/ui/src/views/sessions/sessionsPage.tsx:
  19  
  20: import { SessionsPage } from "@cockroachlabs/cluster-ui";
  21  import {

pkg/ui/src/views/shared/components/sortabletable/index.tsx:
  289                      // TODO (koorosh): `title` field has ReactNode type isn't correct field to
  290:                     // track column name. `SortableColumn` has to be imported from `@cockroachlabs/cluster-ui`
  291                      // package which has extended field to track column name.

pkg/ui/src/views/statements/statementDetails.tsx:
  42    AggregateStatistics,
  43: } from "@cockroachlabs/cluster-ui";
  44  import { createStatementDiagnosticsReportAction } from "src/redux/statements";

pkg/ui/src/views/statements/statementsPage.tsx:
  35  
  36: import { StatementsPage, AggregateStatistics } from "@cockroachlabs/cluster-ui";
  37  import {

pkg/ui/src/views/transactions/transactionsPage.tsx:
  21  
  22: import { TransactionsPage } from "@cockroachlabs/cluster-ui";
```

## Drawbacks

WIP

Drawbacks might be that since a single component isn't shared in two applications there may be some duplication or features may have to be written twice. This seems like a fair and potentially unavoidable reality that will be easier to deal with than what we have now.
