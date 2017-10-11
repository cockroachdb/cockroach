// charts.js
//
// Draw time series charts based on configuration and data.
//
// For now, the usage should look something like the following.
//
//    var page = charts.config({
//      el: document.getElementById("element-to-render-into"),
//      charts: [
//        {
//          title: "My Fancy Chart",
//          metrics: [
//            title: "Total Foos",
//            series: "foo.count"
//          ]
//        }
//      ]
//    });
//    var data = {
//      "foo.count": [
//        { timestamp_nanos: 123, value: 456 }
//      ]
//    }
//    charts.render(page, data);

var margin = {
  left: 55,
  top: 30,
  right: 25,
  bottom: 20
};

// This is backwards from the global below, because the data is shared among all
// charts, but each one has its own configuration.  Confusing, and we can
// probably restructure it to make more sense, but for now it works.
function renderChart(data) {
  return function configChart(config, index) {
    var container = d3.select(this);

    var rect = container.node().getBoundingClientRect();

    var svg = container.selectAll("svg.graph")
      .attr("width", rect.width)
      .attr("height", rect.height);

    var chart = svg.selectAll("g.main")
      .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var width = rect.width - margin.left - margin.right;
    var height = rect.height - margin.top - margin.bottom;

    chart.append("defs").append("clipPath")
      .attr("id", "clip" + index)
      .append("rect")
      .attr("width", width)
      .attr("height", height);

    var x = d3.scaleUtc().range([0, width]);
    var y = d3.scaleLinear().range([height, 0]);

    var color = d3.scaleOrdinal(d3.schemeCategory10);

    var xAxis = d3.axisBottom(x)
      .ticks(width < 700 ? 5 : 10)
      .tickSizeInner(-height)
      .tickSizeOuter(0);
    var yAxis = d3.axisLeft(y)
      .ticks(2)
      .tickSizeInner(-width)
      .tickSizeOuter(0);

    var line = d3.line()
      .x(function (d) { return x(d.timestamp_nanos); })
      .y(function (d) { return y(d.value); });

    var visibleData = {};
    config.metrics.forEach(function (m) {
      visibleData[m.series] = data.series[m.series].filter(function (d) {
        return d.timestamp_nanos >= data.range[0] - 1000 && d.timestamp_nanos <= data.range[1] + 1000;
      });
    });

    var yExtents = config.metrics.map(function (m) { return d3.extent(visibleData[m.series], function (d) { return d.value; }); });

    x.domain(data.range);
    y.domain([d3.min(yExtents, function (d) { return d[0]; }), d3.max(yExtents, function (d) { return d[1]; })]);

    var legend = chart.selectAll("g.nv-legend")
      .attr("transform", "translate(" + (width - config.metrics.length * 70) + ",10)");

    var legendsBase = legend.selectAll("g.nv-series")
      .data(config.metrics.length === 1 ? [] : config.metrics);

    var legendsNew = legendsBase.enter()
      .append("g")
      .attr("class", "nv-series");

    legendsNew.append("circle")
      .attr("class", "nv-legend-symbol")
      .attr("r", 3);

    legendsNew.append("text")
      .attr("class", "nv-legend-text")
      .attr("text-anchor", "start")
      .attr("dx", 8)
      .attr("dy", "0.32em")
      .attr("fill", "#000");

    var legends = legendsNew
      .merge(legendsBase);

    legends
      .attr("transform", function (m, i) { return "translate(" + (i*71) + ",5)"; })
      // TODO(couchand): revisit these handler methods
      .on("dblclick", function (m) {
        config.metrics.forEach(function (other) {
          other.hidden = true;
        });
        m.hidden = false;

        container.each(renderChart);
      })
      .on("click", function (m) {
        m.hidden = !m.hidden;

        var allHidden = true;
        config.metrics.forEach(function (other) {
          if (!other.hidden) {
            allHidden = false;
          }
        });
        if (allHidden) {
          config.metrics.forEach(function (other) {
            other.hidden = false;
          });
        }

        container.each(configChart);
      });

    legends.select("circle")
      .attr("stroke", function (m) { return color(m.title); })
      .style("stroke-width", "2px")
      .attr("fill", function (m) { return color(m.title); })
      .style("fill-opacity", function (m) { return m.hidden ? 0 : 1; });

    legends.select("text")
      .text(function (m) { return m.title; });

    var xContainer = chart.select("g.nv-x")
      .attr("transform", "translate(0," + height + ")");

    xContainer
      .selectAll(".nv-axis")
      .interrupt()
      .transition()
      .duration(1000)
      .ease(d3.easeLinear)
      .call(xAxis);

    var yContainer = chart.select("g.nv-y");

    yContainer
      .selectAll(".nv-axis")
      .call(yAxis);

    yContainer
      .selectAll("text.nv-axislabel")
      .attr("x", -height / 2)
      .text(config.type);

    var linesContainer = chart.selectAll("g.lines")
      .attr("clip-path", "url(#clip" + index + ")");

    var linesBase = linesContainer.selectAll("path.line")
      .data(config.metrics);

    var lines = linesBase.enter()
      .append("path")
      .attr("class", "line")
      .attr("stroke", "steelblue")
      .attr("fill", "none")
      .merge(linesBase);

    lines
      .attr("stroke", function (m) { return color(m.title); })
      .style("stroke-opacity", function (m) { return m.hidden ? 0 : 1; })
      .attr("d", function (m) { return line(visibleData[m.series]); });

    lines
      .attr("transform", null)
      .interrupt("x-axis")
      .transition("x-axis")
      .duration(1000)
      .ease(d3.easeLinear)
      .attr("transform", "translate(" + (x(-1000) - x(0)) + ")");
  };
}

function configChartArray(config) {
  return function renderChartArray(data) {
    var div = d3.select(config.el);

    var visualizationBase = div.selectAll("div.visualization")
      .data(config.charts);

    var visualizationEnter = visualizationBase.enter()
      .append("div")
      .attr("class", "visualization");

    visualizationEnter
      .append("div")
      .attr("class", "visualization__header")
      .append("span")
      .attr("class", "visualization__title");

    var chartEnter = visualizationEnter
      .append("div")
      .attr("class", "visualization__content")
      .append("div")
      .attr("class", "linegraph nvd3")
      // TODO(couchand): the rest of this line as well as all the following
      // initialization should be inside the chart fn
      .append("svg")
      .attr("class", "graph nvd3-svg")
      .append("g")
      .attr("class", "main");

    var legend = chartEnter.append("g")
      .attr("class", "nv-legendWrap")
      .attr("transform", "translate(0,-30)")
      .append("g")
      .attr("class", "nvd3 nv-legend");

    chartEnter.append("g")
      .attr("class", "nvd3-svg nv-x nv-axis")
      .append("g")
      .attr("class", "nvd3 nv-wrap nv-axis");
    chartEnter.append("g")
      .attr("class", "nvd3-svg nv-y nv-axis")
      .append("g")
      .attr("class", "nvd3 nv-wrap nv-axis")
      .append("text")
      .attr("class", "nv-axislabel")
      .attr("transform", "rotate(-90)")
      .attr("y", "-40") // TODO(couchand): de-magicify this number
      .style("text-anchor", "middle");

    chartEnter.append("g")
      .attr("class", "lines");

    var visualization = visualizationEnter.merge(visualizationBase);

    var header = visualization.selectAll(".visualization__header");
    var title = header.selectAll(".visualization__title");

    title.text(function (d) { return d.title; });

    var content = visualization.selectAll(".visualization__content");
    var linegraph = content.selectAll(".linegraph");

    var renderWithData = renderChart(data);

    linegraph.each(renderWithData);

    // TODO(couchand): apparently, the current flexbox styling is getting in the
    // way of the resize being handled properly -- needs investigation
    // TODO(couchand): no dependency for debounce?
    window.addEventListener("resize", _.debounce(function () {
      linegraph.each(renderWithData);
    }, 10));
  };
}

function renderChartWithData(chart, data) {
  chart(data);
}

window.charts = {
  configure: configChartArray,
  render: renderChartWithData
};
