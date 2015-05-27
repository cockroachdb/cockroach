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
                query:Models.Metrics.QueryManager;
                lastEpoch:number;
                key?:number;
            }

            /**
             * Controller contains the bulk of functionality needed to render a
             * LineGraph.
             */
            class Controller {
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
                    // Set xAxis ticks to properly format.
                    this.chart.xAxis
                        .tickFormat(d3.time.format('%I:%M:%S'))
                        .showMaxMin(false);
                }

                /**
                 * shouldRenderData returns true if there is new data to render.
                 */
                shouldRenderData():boolean {
                    var epoch = this.vm.query.epoch()
                    if (epoch > this.vm.lastEpoch) {
                        this.vm.lastEpoch = epoch
                        return true;
                    }
                    return false;
                }

                /** 
                 * hasData returns true if graph data is available to render.
                 */
                hasData():boolean {
                    return this.vm.query.epoch() > 0
                }

                /** 
                 * drawGraph gets direct access to the svg element of the graph
                 * after it is added to DOM. We use NVD3 to draw the data from
                 * the query.
                 */
                drawGraph = (element:Element, isInitialized:boolean, context:any) => {
                    if (!isInitialized) {
                        nv.addGraph(this.chart);
                    } 

                    if (this.shouldRenderData()) {
                        var formattedData = []
                        // The result() property will be empty if an error
                        // occured. For now, we will just display the "No Data"
                        // message until we decided on the proper way to display
                        // error messages.
                        if (this.vm.query.result()) {
                            formattedData = this.vm.query.result().results.map((d) => {
                                return {
                                    values: d.datapoints,
                                    key: d.name,
                                    color: Controller.colors(d.name),
                                    area:true,
                                    fillOpacity:.1,
                                };
                            });
                        }
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
                if (ctrl.hasData()) {
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
            export function create(query:Models.Metrics.QueryManager, key?:number){
                var vm:ViewModel = {lastEpoch:0, query:query}
                if (key) {
                    vm.key = key;
                }
                return m.component(LineGraph, vm)
            }
        }
    }
}
