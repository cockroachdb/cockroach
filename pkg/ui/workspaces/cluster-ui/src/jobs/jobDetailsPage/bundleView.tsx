// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import classnames from "classnames/bind";
import { Button } from "@cockroachlabs/ui-components";
import emptyListResultsImg from "src/assets/emptyState/empty-list-results.svg";
import { EmptyTable } from "src/empty";
import styles from "./bundleView.module.scss";
import { ColumnDescriptor, SortedTable, SortSetting } from "src/sortedtable";
import { DATE_FORMAT_24_TZ } from "../../util";
import { Timestamp } from "../../timestamp";
import moment from "moment-timezone";
import { InsertJobProfilerBundleRequest, JobProfilerBundle } from "src/api/jobProfilerBundleApi";

export interface BundleViewStateProps {
    bundles: JobProfilerBundle[];
}

export interface BundleViewDispatchProps {
    onCollectJobProfilerBundle: (
        insertJobProfilerBundleRequest: InsertJobProfilerBundleRequest,
    ) => void;
}

export interface BundleViewOwnProps {
    jobID?: string;
}

export type BundleViewProps = BundleViewOwnProps &
    BundleViewStateProps &
    BundleViewDispatchProps;

interface BundleViewState {
    sortSetting: SortSetting;
}

const cx = classnames.bind(styles);

const NavButton: React.FC = props => (
    <Button {...props} as="a" intent="tertiary">
        {props.children}
    </Button>
);

export class BundleView extends React.Component<
    BundleViewProps,
    BundleViewState
> {
    constructor(props: BundleViewProps) {
        super(props);
        this.state = {
            sortSetting: {
                ascending: true,
                columnTitle: "activatedOn",
            },
        };
    }

    columns: ColumnDescriptor<JobProfilerBundle>[] = [
        {
            name: "activatedOn",
            title: "Activated on",
            hideTitleUnderline: true,
            cell: (bundle: JobProfilerBundle) => (
                <Timestamp time={bundle.written} format={DATE_FORMAT_24_TZ} />
            ),
            sort: (bundle: JobProfilerBundle) =>
                moment(bundle.written)?.unix(),
        },
    ];

    render(): React.ReactElement {
        const {
            jobID,
            bundles,
        } = this.props;

        return (
            <>
                <div className={cx("crl-statements-diagnostics-view__header")}>
                    {(
                        <Button
                            onClick={() =>
                                this.props.onCollectJobProfilerBundle({ jobID: jobID })
                            }
                            intent="secondary"
                        >
                            Collect bundle
                        </Button>
                    )}
                </div>
                <SortedTable
                    data={bundles}
                    columns={this.columns}
                    className={cx("jobs-table")}
                    sortSetting={this.state.sortSetting}
                    tableWrapperClassName={cx("sorted-table")}
                />
            </>
        );
    }
}
