import * as React from "react";
import { assert } from "chai";
import { shallow } from "enzyme";
import _ = require("lodash");
import Long = require("long");
import * as sinon from "sinon";

import * as protos from  "../js/protos";
import { TextGraph, Axis, Metric } from "../components/graphs";
import { MetricsDataProviderUnconnected as MetricsDataProvider } from "./metricsDataProvider";
import { MetricsQuery } from "../redux/metrics";

type TSRequestMessage = cockroach.ts.TimeSeriesQueryRequestMessage;

function makeDataProvider(id: string,
                          metrics: MetricsQuery,
                          timeSpan: Long[],
                          queryMetrics: (id: string, request: TSRequestMessage) => void) {
  return shallow(<MetricsDataProvider id={id} metrics={metrics} timeSpan={timeSpan} queryMetrics={queryMetrics}>
    <TextGraph>
      <Axis>
        <Metric name="test.metric.1" />
        <Metric name="test.metric.2" />
      </Axis>
      <Axis>
        <Metric name="test.metric.3" />
      </Axis>
    </TextGraph>
  </MetricsDataProvider>);
}

function makeMetricsRequest(timeSpan: Long[]) {
  return new protos.cockroach.ts.TimeSeriesQueryRequest({
    start_nanos: timeSpan[0],
    end_nanos: timeSpan[1],
    queries: [
      {
        name: "test.metric.1",
        sources: undefined,
        downsampler: cockroach.ts.TimeSeriesQueryAggregator.AVG,
        source_aggregator: cockroach.ts.TimeSeriesQueryAggregator.SUM,
        derivative: cockroach.ts.TimeSeriesQueryDerivative.NONE,
      },
      {
        name: "test.metric.2",
        sources: undefined,
        downsampler: cockroach.ts.TimeSeriesQueryAggregator.AVG,
        source_aggregator: cockroach.ts.TimeSeriesQueryAggregator.SUM,
        derivative: cockroach.ts.TimeSeriesQueryDerivative.NONE,
      },
      {
        name: "test.metric.3",
        sources: undefined,
        downsampler: cockroach.ts.TimeSeriesQueryAggregator.AVG,
        source_aggregator: cockroach.ts.TimeSeriesQueryAggregator.SUM,
        derivative: cockroach.ts.TimeSeriesQueryDerivative.NONE,
      },
    ],
  });
}

function makeMetricsQuery(id: string, timeSpan: Long[]): MetricsQuery {
  let request = makeMetricsRequest(timeSpan);
  let data = new protos.cockroach.ts.TimeSeriesQueryResponse({
    results: _(request.queries).map((q) => {
      return {
        query: q,
        datapoints: [],
      };
    }).value(),
  });
  return {
    id,
    request,
    data,
    error: undefined,
  };
}

describe("<MetricsDataProvider>", function() {
  let spy: sinon.SinonSpy;
  let timespan1 = [Long.fromNumber(0), Long.fromNumber(100)];
  let timespan2 = [Long.fromNumber(100), Long.fromNumber(200)];
  let graphid = "testgraph";

  beforeEach(function() {
    spy = sinon.spy();
  });

  describe("refresh", function() {
    it("refreshes query data when mounted.", function () {
      makeDataProvider(graphid, null, timespan1, spy);
      assert.isTrue(spy.called);
      assert.isTrue(spy.calledWith(graphid, makeMetricsRequest(timespan1)));
    });

    it("does nothing when mounted if current request fulfilled.", function () {
      makeDataProvider(graphid, makeMetricsQuery(graphid, timespan1), timespan1, spy);
      assert.isTrue(spy.notCalled);
    });

    it("refreshes query data when receiving props.", function () {
      let provider = makeDataProvider(graphid, makeMetricsQuery(graphid, timespan1), timespan1, spy);
      assert.isTrue(spy.notCalled);
      provider.setProps({
        metrics: undefined,
      });
      assert.isTrue(spy.called);
      assert.isTrue(spy.calledWith(graphid, makeMetricsRequest(timespan1)));
    });

    it("refreshes if timespan changes.", function () {
      let provider = makeDataProvider(graphid, makeMetricsQuery(graphid, timespan1), timespan1, spy);
      assert.isTrue(spy.notCalled);
      provider.setProps({
        timeSpan: timespan2,
      });
      assert.isTrue(spy.called);
      assert.isTrue(spy.calledWith(graphid, makeMetricsRequest(timespan2)));
    });

  });

  describe("attach", function() {
    it("attaches metrics data to contained component", function() {
      let provider = makeDataProvider(graphid, makeMetricsQuery(graphid, timespan1), timespan1, spy);
      let props: any = provider.first().props();
      assert.isDefined(props.data);
      assert.deepEqual(props.data, makeMetricsQuery(graphid, timespan1).data);
    });

    it("throws error if it contains multiple graph components", function() {
      try {
        shallow(
          <MetricsDataProvider id="id"
                               metrics={null}
                               timeSpan={timespan1}
                               queryMetrics={spy}>
            <TextGraph>
              <Axis>
                <Metric name="test.metrics.1" />
              </Axis>
            </TextGraph>
            <TextGraph>
              <Axis>
                <Metric name="test.metrics.2" />
              </Axis>
            </TextGraph>
          </MetricsDataProvider>
        );
        assert.fail("expected error from MetricsDataProvider");
      } catch (e) {
        assert.match(e, /Invariant Violation/);
      }
    });
  });
});

