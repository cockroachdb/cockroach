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
     * nv charts module does not currently have a typescript definition.
     */
    declare var nv:any;

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
             * Controller contains the bulk of functionality needed to render a
             * LineGraph.
             */
            class Controller {
                // activeQuery is the promised results of a currently active
                // query.
                activeQuery:_mithril.MithrilPromise<Models.Metrics.QueryResultSet>;
                // data contains the current data set rendered to the graph.
                data = m.prop(<Models.Metrics.QueryResultSet> null);
                // error returns any error which occurred during the previous query.
                error = m.prop(<Error> null);

                // nvd3 chart.
                chart = nv.models.lineChart()
                    .x((d) => new Date(d.timestamp_nanos/1.0e6))
                    .y((d) => d.value)
                    //.interactive(true)
                    .useInteractiveGuideline(true) 
                    .showLegend(true)
                    .showYAxis(true)
                    .showXAxis(true)
                    .xScale(d3.time.scale());

                static colors = d3.scale.category10();

                constructor(public vm:ViewModel) {
                    // Query for initial result set.
                    this.queryData();

                    // Set xAxis ticks to properly format.
                    this.chart.xAxis
                        .tickFormat(d3.time.format('%I:%M:%S'))
                        .showMaxMin(false);
                }

                queryData() {
                    if (this.activeQuery) {
                        return;
                    }
                    this.error(null);
                    this.activeQuery = this.vm.query.query().then(null, this.error)
                }

                /**
                 * readData reads the results of a completed active query.
                 */
                readData():boolean {
                    // If there is an outstanding query and it has completed,
                    // move the data into our internal property.
                    if (this.activeQuery && this.activeQuery()) {
                        this.data(this.activeQuery());
                        this.activeQuery = null;
                        return true;
                    }
                    return false;
                }

                /** 
                 * hasData returns true if graph data is available to render.
                 */
                hasData():boolean {
                    return !!this.data() || (this.activeQuery && !!this.activeQuery()); 
                }

                /** 
                 * drawGraph gets direct access to the svg element of the graph
                 * after it is added to DOM. We use NVD3 to draw the data from
                 * the query.
                 */
                drawGraph = (element:Element, isInitialized:boolean, context:any) => {
                    if (!isInitialized) {
                        var interval = setInterval(() => this.queryData(), 10000);
                        context.onunload = () => {
                            clearInterval(interval);
                        }
                        nv.addGraph(this.chart);
                    } 

                    if (this.readData()) {
                        var formattedData = this.data().results.map((d) => {
                            return {
                                values: d.datapoints,
                                key: d.name,
                                color: Controller.colors(d.name),
                                area:true,
                                fillOpacity:.1,
                            };
                        });
                        d3.select(element)
                            .datum(formattedData)
                        .transition().duration(500)
                            .call(this.chart);
                    }
                }
            }

            export function controller(model:ViewModel) {
                return new Controller(model);
            }

            export function view(ctrl:Controller) {
                if (ctrl.error()) {
                    return m("", "error loading graph:" + ctrl.error());
                } else if (ctrl.hasData()) {
                    return m(".linegraph", 
                            {style:"width:500px;height:300px;"},
                            m("svg.graph", {config: ctrl.drawGraph})
                            );
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
                var vm:ViewModel = {width:width, height:height, lastVersion:0, query:query}
                if (key) {
                    vm.key = key;
                }
                return m.component(LineGraph, vm)
            }
        }
    }
}
