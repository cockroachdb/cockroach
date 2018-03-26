// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import React from "react";

import { withRouter, WithRouterProps } from "react-router";

import { connect } from "react-redux";
import { refreshNodes } from "src/redux/apiReducers";
import { nodesSummarySelector, NodesSummary } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";

import { cockroach, io } from "src/js/protos";
import {
  CustomChartState,
  CustomMetricState,
} from "src/views/reports/containers/customChart/customMetric";

import { AxisUnits } from "src/views/shared/components/metricQuery";

import "./chartCatalog.styl";

// OVERVIEW
// This page provides users with a predefined set of charts (which we call a
// catalog) that can aid them in troubleshooting their cluster. This catalog
// is available at /_admin/v1/chartcatalog, and is defined in pkg/ts/catalog.
//
// This page renders the catalog in HTML. When users click links, the catalog
// is scanned for all predefined charts that belong to that collection, i.e.
// all IndividualCharts that are defined as children of the given collection.
//
// This set of charts is then converted into a URL parameter for the
// customChart page, and then that page is loaded.

export interface ChartCatalogProps {
  nodesSummary: NodesSummary;
  refreshNodes: typeof refreshNodes;
}

type ChartCatalogState = {
  error: Error;
  isLoaded: boolean;
  catalog: cockroach.ts.catalog.ChartSection[];
  storeMetrics: string[];
};

type CatalogAndStoreMetrics = {
  catalog: cockroach.ts.catalog.ChartSection[];
  // storeMetrics is used to determine the appropriate prefix for the metric,
  // based on where it's a store metric (cr.store) or a node metric (cr.node).
  storeMetrics: string[];
};

class ChartCatalog extends React.Component<
  ChartCatalogProps & WithRouterProps,
  ChartCatalogState
> {
  refresh(props = this.props) {
    props.refreshNodes();
  }

  constructor(props: ChartCatalogProps & WithRouterProps) {
    super(props);

    this.state = {
      error: null,
      isLoaded: false,
      catalog: [],
      storeMetrics: [],
    };
  }

  componentWillMount() {
    this.refresh();
  }

  componentWillReceiveProps(props: ChartCatalogProps & WithRouterProps) {
    this.refresh(props);
  }

  componentDidMount() {
    // Either get the list of store metrics or refresh.
    let storeMetrics: string[] = [];
    this.props.nodesSummary.nodeStatuses
      ? (storeMetrics = _.keys(
          this.props.nodesSummary.nodeStatuses[0].store_statuses[0].metrics,
        ))
      : this.refresh();
    this.setState({
      storeMetrics: storeMetrics,
    });

    fetch("/_admin/v1/chartcatalog")
      .then(res => res.json())
      .then(
        result => {
          this.setState({
            isLoaded: true,
            catalog: result.catalog,
          });
        },
        error => {
          this.setState({
            isLoaded: true,
            error,
          });
        },
      );
    window.scrollTo(0, 0);
  }

  render() {
    const { error, isLoaded, catalog, storeMetrics } = this.state;
    if (error) {
      return <div>Error: {error.message}</div>;
    } else if (!isLoaded) {
      return <div>Loading...</div>;
    } else {
      const cat: CatalogAndStoreMetrics = {
        catalog: catalog,
        storeMetrics: storeMetrics,
      };
      return (
        <section className="section">
          <h1>Chart Catalog</h1>
          <p className="catalog-description">
            Understand your cluster's behavior with this curated set of charts.
            <br />
            <br />
            <strong className="catalog-link">Bold</strong> links return all •{" "}
            <span className="catalog-link">Charts</span> under their{" "}
            <span className="overhang">overhang.</span>
          </p>
          <div className="catalog">
            {_.map(catalog, (sec, key) => (
              <RenderSection section={sec} key={key} cat={cat} />
            ))}
          </div>
        </section>
      );
    }
  }
}

function mapStateToProps(state: AdminUIState) {
  return {
    nodesSummary: nodesSummarySelector(state),
  };
}

const mapDispatchToProps = {
  refreshNodes,
};

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(withRouter(ChartCatalog));

function RenderSection(props: {
  section: cockroach.ts.catalog.ChartSection;
  key: number;
  cat: CatalogAndStoreMetrics;
}) {
  function createMarkup() {
    return { __html: props.section.description };
  }

  return (
    <div className="catalog-section">
      <table className="catalog-table">
        <tr>
          <td className="catalog-table__section-header">
            <div className="sticky">
              <h2 className="section-title">{props.section.title}</h2>
            </div>
            <p
              className="section-description"
              dangerouslySetInnerHTML={createMarkup()}
            />
          </td>
          <td>
            <div className="catalog-table__chart-links">
              {_.map(props.section.subsections, (sec, key) => (
                <RenderSectionTable
                  section={sec as cockroach.ts.catalog.ChartSection}
                  key={key}
                  cat={props.cat}
                />
              ))}
            </div>
          </td>
        </tr>
      </table>
    </div>
  );
}

function RenderSectionTable(props: {
  section: cockroach.ts.catalog.ChartSection;
  key: number;
  cat: CatalogAndStoreMetrics;
}) {
  function handleClick(e: React.MouseEvent<HTMLAnchorElement>) {
    e.preventDefault();
    window.location.hash = getCustomChartURL(
      props.cat,
      props.section.collectionTitle,
    );
  }

  if (
    props.section.subsections.length === 0 &&
    props.section.charts.length === 0
  ) {
    return "";
  }

  return (
    <div>
      <table
        className={"inner-table catalog-table-level-" + props.section.level}
      >
        <tr>
          <th className="catalog-table__header">
            <a className="catalog-link" href="#" onClick={handleClick}>
              {props.section.title}
            </a>
          </th>
          <td className="catalog-table__cell">
            <RenderListCell
              links={
                props.section.charts as cockroach.ts.catalog.IndividualChart[]
              }
              cat={props.cat}
            />
            {_.map(props.section.subsections, (sec, key) => (
              <RenderSectionTable
                section={sec as cockroach.ts.catalog.ChartSection}
                key={key}
                cat={props.cat}
              />
            ))}
          </td>
        </tr>
      </table>
    </div>
  );
}

function RenderListCell(props: {
  links: cockroach.ts.catalog.IndividualChart[];
  cat: CatalogAndStoreMetrics;
}) {
  return (
    <div>
      <ul>
        {_.map(props.links, link => (
          <li key={link.collectionTitle}>
            <RenderCustomChartLink link={link} cat={props.cat} />
          </li>
        ))}
      </ul>
    </div>
  );
}

function RenderCustomChartLink(props: {
  link: cockroach.ts.catalog.IndividualChart;
  cat: CatalogAndStoreMetrics;
}) {
  function handleClick(e: React.MouseEvent<HTMLAnchorElement>) {
    e.preventDefault();
    window.location.hash = getCustomChartURL(
      props.cat,
      props.link.collectionTitle,
    );
  }

  return (
    <a className="catalog-link" href="#" onClick={handleClick}>
      {props.link.title}
    </a>
  );
}

// getCustomChartURL returns a URL for the custom charts page that contains URL parameters
// to display all of the charts from the catalog that belong to the specified collection.
function getCustomChartURL(
  cat: CatalogAndStoreMetrics,
  collectionTitle: string,
): string {
  // Create URL
  let customCharts: CustomChartState[] = [];
  getCharts(cat.catalog, collectionTitle).forEach(chart => {
    customCharts = customCharts.concat(
      getCustomChartParams(chart, cat.storeMetrics),
    );
  });
  return (
    "/debug/chart?charts=" + encodeURIComponent(JSON.stringify(customCharts))
  );
}

// getCharts returns all charts that belong to the collection of charts with
// the given name.
function getCharts(
  catalog: cockroach.ts.catalog.ChartSection[],
  name: string,
): cockroach.ts.catalog.IndividualChart[] {
  let collectionToDisplay:
    | cockroach.ts.catalog.ChartSection
    | cockroach.ts.catalog.IndividualChart;
  let i: number = 0;

  // Iterate over all of the top levels of the catalog looking for the collection
  // with the given name.
  while (!collectionToDisplay && i < catalog.length) {
    collectionToDisplay = findCollection(catalog[i], name);
    i++;
  }

  return aggregateCharts(
    collectionToDisplay,
  ) as cockroach.ts.catalog.IndividualChart[];
}

// findCollection searches the catalog for the "collection" with the given name. Collections
// can either be a single chart or multiple charts, represented as either IndividualCharts
// or ChartSections.
function findCollection(
  node:
    | cockroach.ts.catalog.ChartSection
    | cockroach.ts.catalog.IndividualChart,
  collectionName: string,
): cockroach.ts.catalog.ChartSection | cockroach.ts.catalog.IndividualChart {
  if (node.collectionTitle === collectionName) {
    return node;
  }

  let results:
    | cockroach.ts.catalog.ChartSection
    | cockroach.ts.catalog.IndividualChart;

  // Only ChartSections have the property charts; when node is a ChartSection, search its
  // charts and subsections for the collection.
  if (node.hasOwnProperty("charts")) {
    node = node as cockroach.ts.catalog.ChartSection;

    node.charts.some(chart => {
      results = findCollection(
        chart as cockroach.ts.catalog.IndividualChart,
        collectionName,
      );
      return results;
    });

    if (!results) {
      node.subsections.some(subsection => {
        results = findCollection(
          subsection as cockroach.ts.catalog.ChartSection,
          collectionName,
        );
        return results;
      });
    }
  }

  return results;
}

// aggregateCharts collects all of the charts that can be reached from the node.
function aggregateCharts(
  node: cockroach.ts.catalog.ChartSection | cockroach.ts.catalog.IndividualChart,
) {
  let chartsArr: cockroach.ts.catalog.IndividualChart[] = [];

  // Only IndividualCharts have "metrics"; otherwise the node is a ChartCatalog and should
  // aggregate all of its charts and all of its subsection's charts.
  if (node.hasOwnProperty("metrics")) {
    chartsArr = chartsArr.concat(node as cockroach.ts.catalog.IndividualChart);
  } else {
    chartsArr = chartsArr.concat(
      aggregateChartsFromSection(node as cockroach.ts.catalog.ChartSection),
    );
  }

  return chartsArr;
}

// aggregateChartsFromSection aggregates all of a section's IndividualCharts, as well
// as all of its subsection's IndividualCharts.
function aggregateChartsFromSection(
  section: cockroach.ts.catalog.ChartSection,
) {
  // Aggregate all of the section's IndividualCharts.
  let chartsArr: cockroach.ts.catalog.IndividualChart[] = section.charts as cockroach.ts.catalog.IndividualChart[];

  // Aggregate all of the section's subsections' IndividualCharts.
  section.subsections.forEach(
    (subsection: cockroach.ts.catalog.ChartSection) => {
      chartsArr = chartsArr.concat(
        aggregateChartsFromSection(
          subsection as cockroach.ts.catalog.ChartSection,
        ),
      );
    },
  );

  return chartsArr;
}

// axisUnitsMap converts between the enums in cockroach.ts.catalog and those in
// in metricQuery/index.tsx.
const axisUnitsMap = {
  [cockroach.ts.catalog.AxisUnits.BYTES]: AxisUnits.Bytes,
  [cockroach.ts.catalog.AxisUnits.COUNT]: AxisUnits.Count,
  [cockroach.ts.catalog.AxisUnits.DURATION]: AxisUnits.Duration,
  [cockroach.ts.catalog.AxisUnits.UNSET_UNITS]: AxisUnits.Count,
};

// getCustomChartParams converts cockroach.ts.catalog.IndividualChart into
// objects that can be displayed as custom charts in the admin UI by
// passing them in as URL parameters.
function getCustomChartParams(
  chart: cockroach.ts.catalog.IndividualChart,
  storeMetrics: string[],
): CustomChartState {
  const customChart: CustomChartState = {
    metrics: [],
    axisUnits: axisUnitsMap[chart.units],
  };

  const quantileSuffixes = [
    "-max",
    "-p99.999",
    "-p99.99",
    "-p99.9",
    "-p99",
    "-p90",
    "-p75",
    "-p50",
  ];

  chart.metrics.forEach(metric => {
    const metricName: string = _.includes(storeMetrics, metric.name)
      ? "cr.store." + metric.name
      : "cr.node." + metric.name;

    if (metric.metricType === io.prometheus.client.MetricType.HISTOGRAM) {
      // Append each quantile suffix to the metric name before creating the CustomMetricState.
      quantileSuffixes.forEach(suffix => {
        customChart.metrics = customChart.metrics.concat(
          createCustomMetricState(chart, metricName + suffix),
        );
      });
    } else {
      customChart.metrics = customChart.metrics.concat(
        createCustomMetricState(chart, metricName),
      );
    }
  });

  return customChart;
}

// createCustomMetricState essentially converts a single metric from an IndividualChart
// into a CustomMetricState. This is broken out into its own function for simplicity's
// sake in generating the correct metric name for histogram metrics, which requires
// appending a set of sufffixes.
function createCustomMetricState(
  chart: cockroach.ts.catalog.IndividualChart,
  metricName: string,
): CustomMetricState {
  const metricVal: CustomMetricState = {
    downsampler: chart.downsampler,
    aggregator: chart.aggregator,
    derivative: chart.derivative,
    perNode: false,
    source: "",
    metric: metricName,
  };
  return metricVal;
}
