import React from "react";
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { makeStatementsColumns } from "../statementsTable";
import {
  SortedTable,
  ISortedTablePagination,
  SortSetting,
} from "../sortedtable";
import { Pagination } from "../pagination";
import { TransactionsPageStatistic } from "../transactionsPage/transactionsPageStatistic";
import { baseHeadingClasses } from "../transactionsPage/transactionsPageClasses";
import { Button } from "../button";
import { collectStatementsText } from "src/transactionsPage/utils";
import { tableClasses } from "../transactionsTable/transactionsTableClasses";
import { BackIcon } from "../icon";
import { SqlBox } from "../sql";
import { aggregateStatements } from "../transactionsPage/utils";
import Long from "long";
import { Loading } from "../loading";

const { containerClass } = tableClasses;

type Statement = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;

interface TransactionDetailsProps {
  statements?: Statement[];
  lastReset?: string | Date;
  handleDetails: (statementIds: Long[] | null) => void;
  error?: Error | null;
}

interface TState {
  sortSetting: SortSetting;
  pagination: ISortedTablePagination;
}

export class TransactionDetails extends React.Component<
  TransactionDetailsProps,
  TState
> {
  state: TState = {
    sortSetting: {
      sortKey: 2,
      ascending: false,
    },
    pagination: {
      pageSize: 10,
      current: 1,
    },
  };

  onChangeSortSetting = (ss: SortSetting) => {
    this.setState({
      sortSetting: ss,
    });
  };

  onChangePage = (current: number) => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current } });
  };

  render() {
    const { statements, handleDetails, error } = this.props;
    return (
      <div>
        <section className={baseHeadingClasses.wrapper}>
          <Button
            onClick={() => handleDetails(null)}
            type="unstyled-link"
            size="small"
            icon={BackIcon}
            iconPosition="left"
          >
            All transactions
          </Button>
          <h1 className={baseHeadingClasses.tableName}>Transaction Details</h1>
        </section>
        <Loading
          error={error}
          loading={!statements}
          render={() => {
            const { statements, lastReset } = this.props;
            const { sortSetting, pagination } = this.state;
            const statementsSummary = collectStatementsText(statements);
            const aggregatedStatements = aggregateStatements(statements);
            return (
              <React.Fragment>
                <section className={containerClass}>
                  <SqlBox value={statementsSummary} />
                  <TransactionsPageStatistic
                    pagination={pagination}
                    totalCount={statements.length}
                    lastReset={lastReset}
                    arrayItemName={"statements for this transaction"}
                    activeFilters={0}
                  />
                  <SortedTable
                    data={aggregatedStatements}
                    columns={makeStatementsColumns(
                      aggregatedStatements,
                      "",
                      "",
                    )}
                    className="statements-table"
                    sortSetting={sortSetting}
                    onChangeSortSetting={this.onChangeSortSetting}
                  />
                </section>
                <Pagination
                  pageSize={pagination.pageSize}
                  current={pagination.current}
                  total={statements.length}
                  onChange={this.onChangePage}
                />
              </React.Fragment>
            );
          }}
        />
      </div>
    );
  }
}
