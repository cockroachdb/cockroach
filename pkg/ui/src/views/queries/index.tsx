// tslint:disable-next-line:no-var-requires
const spinner = require<string>("assets/spinner.gif");
// tslint:disable-next-line:no-var-requires
const noResults = require<string>("assets/noresults.svg");

import _ from "lodash";
import React from "react";
import { connect } from "react-redux";

import * as protos from "src/js/protos";
import { queriesKey, refreshQueries } from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { TimestampToMoment } from "src/util/convert";
import docsURL from "src/util/docs";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import Loading from "src/views/shared/components/loading";
import { PageConfig, PageConfigItem } from "src/views/shared/components/pageconfig";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { ColumnDescriptor, SortedTable } from "src/views/shared/components/sortedtable";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";

type Query = protos.cockroach.server.serverpb.QueriesResponse.Query;

const QueriesRequest = protos.cockroach.server.serverpb.QueriesRequest;

const showOptions = [
  { value: "50", label: "Latest 50" },
  { value: "0", label: "All" },
];

const showSetting = new LocalSetting<AdminUIState, string>(
  "jobs/show_setting", s => s.localSettings, showOptions[0].value,
);

// Specialization of generic SortedTable component:
//   https://github.com/Microsoft/TypeScript/issues/3960
//
// The variable name must start with a capital letter or JSX will not recognize
// it as a component.
// tslint:disable-next-line:variable-name
const QueriesSortedTable = SortedTable as new () => SortedTable<Query>;

// tslint:disable-next-line:variable-name
const QueriesTableColumns: ColumnDescriptor<Query>[] = [
  {
    title: "Query ID",
    cell: query => query.query_id,
    sort: query => query.query_id,
  },
  {
    title: "NodeID",
    cell: query => String(query.node_id),
    sort: query => query.node_id,
  },
  {
    title: "Query",
    cell: query => query.query,
    sort: query => query.query,
    className: "queries-table__query-col",
  },
  {
    title: "Client Address",
    cell: query => query.client_address,
    sort: query => query.client_address,
  },
  {
    title: "Application Name",
    cell: query => query.application_name,
    sort: query => query.application_name,
  },
  {
    title: "User",
    cell: query => query.username,
    sort: query => query.username,
  },
  {
    title: "Start Time",
    cell: query => TimestampToMoment(query.start).fromNow(),
    sort: query => TimestampToMoment(query.start).valueOf(),
  },
];

const sortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "jobs/sort_setting",
  s => s.localSettings,
  { sortKey: 2 /* creation time */, ascending: false },
);

interface QueriesTableProps {
  sort: SortSetting;
  status: string;
  show: string;
  type: number;
  setSort: (value: SortSetting) => void;
  setShow: (value: string) => void;
  refreshQueries: typeof refreshQueries;
  queries: Query[];
  queriesValid: boolean;
}

const titleTooltip = (
  <span>
    Some jobs can be paused or canceled through SQL. For details, view the docs
    on the <a href={docsURL("pause-job.html")}><code>PAUSE JOB</code></a> and <a
      href={docsURL("cancel-job.html")}><code>CANCEL JOB</code></a> statements.
  </span>
);

class QueriesTable extends React.Component<QueriesTableProps, {}> {
  static title() {
    return (
      <div>
        Queries
        <div className="section-heading__tooltip">
          <ToolTipWrapper text={titleTooltip}>
            <div className="section-heading__tooltip-hover-area">
              <div className="section-heading__info-icon">!</div>
            </div>
          </ToolTipWrapper>
        </div>
      </div>
    );
  }

  refresh(props = this.props) {
    props.refreshQueries(new QueriesRequest({
      status: props.status,
      type: props.type,
      limit: parseInt(props.show, 10),
    }));
  }

  componentWillMount() {
    this.refresh();
  }

  componentWillReceiveProps(props: QueriesTableProps) {
    this.refresh(props);
  }

  onShowSelected = (selected: DropdownOption) => {
    this.props.setShow(selected.value);
  }

  render() {
    const data = this.props.queries && this.props.queries.length > 0 && this.props.queries;
    return <div className="queries-page">
      <div>
        <PageConfig>
          <PageConfigItem>
            <Dropdown
              title="Show"
              options={showOptions}
              selected={this.props.show}
              onChange={this.onShowSelected}
            />
          </PageConfigItem>
        </PageConfig>
      </div>
      <Loading loading={_.isNil(this.props.queries)} className="loading-image loading-image__spinner" image={spinner}>
        <Loading loading={_.isEmpty(data)} className="loading-image" image={noResults}>
          <section className="section">
            <QueriesSortedTable
              data={data}
              sortSetting={this.props.sort}
              onChangeSortSetting={this.props.setSort}
              className="queries-table"
              columns={QueriesTableColumns}
            />
          </section>
        </Loading>
      </Loading>
    </div>;
  }
}

const mapStateToProps = (state: AdminUIState) => {
  const sort = sortSetting.selector(state);
  const show = showSetting.selector(state);
  const key = queriesKey(parseInt(show, 10));
  const queries = state.cachedData.queries[key];
  return {
    sort, show,
    queries: queries && queries.data && queries.data.queries,
    queriesValid: queries && queries.valid,
  };
};

const actions = {
  setSort: sortSetting.set,
  setShow: showSetting.set,
  refreshQueries,
};

export default connect(mapStateToProps, actions)(QueriesTable);
