// source: components/metrics.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../typings/d3/d3.d.ts" />
/// <reference path="../models/timeseries.ts" />

/** 
 * Components defines reusable components which may be used on multiple pages,
 * or multiple times on the same page.
 */
module Components {
    /**
     * Metrics contains components used to display metrics data.
     */
    export module Metrics {
        /**
         * LineGraph displays a line graph of the data returned from a time
         * series query.
         */
        export module LineGraph {
            /**
             * ViewModel is the model for a specific LineGraph - in addition to
             * the backing Query object, it also maintains other per-component
             * display options.
             */
            interface ViewModel {
                width:number;
                height:number;
                query:Models.Metrics.Query;
                key?:number;
            }

            /**
             * Controller maintains the functionality needed for a single
             * LineGraph.
             */
            class Controller {
                margin = {top:20, right:20, bottom: 30, left:60};

                // Width and height of the area containing the graph lines.
                private chartWidth: number;
                private chartHeight: number;

                // Scales and Axes.
                private timeScale = d3.time.scale();
                private valScale = d3.scale.linear(); 
                private timeAxis = d3.svg.axis().scale(this.timeScale).orient("bottom");
                private valAxis = d3.svg.axis().scale(this.valScale).orient("left");

                // Line data interpreter.
                private line = d3.svg.line()
                    .x((d) => this.timeScale(d.timestamp_nanos/1.0e6))
                    .y((d) => this.valScale(d.value));

                private color = d3.scale.category10();

                // Query results.
                results:_mithril.MithrilPromise<Models.Metrics.QueryResultSet>;
                error = m.prop("");

                constructor(public vm:ViewModel) {
                    this.chartWidth = vm.width - this.margin.left - this.margin.right;
                    this.chartHeight = vm.height - this.margin.top - this.margin.bottom;

                    // Initialize axes.
                    this.timeScale.range([0, this.chartWidth]);
                    this.valScale.range([this.chartHeight, 0]);

                    // Query for initial result set.
                    this.results = vm.query.query().then(null, this.error);
                }

                /** drawGraph gets direct access to the svg element of the graph
                 * after it is added to DOM. When the SVG element is first
                 * initialized, we use D3 to draw the data from the query.
                 */
                drawGraph = (element:Element, isInitialized:boolean, context:any) => {
                    if (!isInitialized) {
                        var data:Models.Metrics.QueryResult[] = this.results().results;

                        // Scale value axis based on data.
                        this.valScale.domain([
                                d3.min(data, (d) => d3.min(d.datapoints, (dp) => dp.value)),
                                d3.max(data, (d) => d3.max(d.datapoints, (dp) => dp.value))
                        ]);
                        this.timeScale.domain([this.vm.query.start, this.vm.query.end]);

                        var svg = d3.select(element)
                        .attr("width", this.vm.width)
                        .attr("height", this.vm.height)
                        .append("g")
                        .attr("transform", 
                                "translate(" + this.margin.left + "," + this.margin.top + ")");
                        svg.append("g")
                            .attr("class", "x axis")
                            .attr("transform", "translate(0," + this.chartHeight + ")") 
                            .call(this.timeAxis);
                        svg.append("g")
                            .attr("class", "y axis")
                            .call(this.valAxis);

                        // append lines
                        svg.selectAll(".line")
                            .data(data)
                            .enter()
                            .append("path")
                            .attr("class", (d,i) => "line line" + i)
                            .attr("d", (d) => this.line(d.datapoints))
                            .style("stroke", (d) => this.color(d.name));
                    }
                }
            }

            export function controller(model:ViewModel) {
                return new Controller(model);
            }

            export function view(ctrl) {
                if (ctrl.error()) {
                    return m("", "error loading graph:" + ctrl.error());
                } else if (ctrl.results()) {    
                    return m("svg.graph", {config: ctrl.drawGraph}); 
                } else {
                    return m("", "loading...");
                }
            }

            /**
             * create instantiates a single instance of the LineGraph component
             * which displays data from the supplied Metrics.Query object.
             *
             * @param key The key param is used by mithril to track objects in lists which can be rearranged.
             */
            export function create(width:number, height:number, query:Models.Metrics.Query, key?:number){
                var vm:ViewModel = {width:width, height:height, query:query}
                if (!!key) {
                    vm.key = key;
                }
                return m.component(LineGraph, vm)
            }
        }
    }
}
