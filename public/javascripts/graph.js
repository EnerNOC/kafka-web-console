/**
 * Copyright (C) 2014 the original author or authors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

function initGraph(graph) {
    graph.fullyUpdated = true;
    graph.editInProgress = false;

    graph.margin = {top: 100, right: 250, bottom: 50, left: 60};
    graph.width = 1160 - graph.margin.left - graph.margin.right;
    graph.height = 400 - graph.margin.top - graph.margin.bottom;

    graph.x = d3.time.scale().range([0,graph.width]);
    graph.y = d3.scale.linear().range([graph.height,0]);
    graph.color = d3.scale.category20();
    graph.xAxis = d3.svg.axis().scale(graph.x).orient("bottom");
    graph.yAxis = d3.svg.axis().scale(graph.y).orient("left");
    graph.line = d3.svg.line().x(function(d) { return graph.x(d[0]); })
                              .y(function(d) { return graph.y(d[1]); });

    graph.beginTime = 0;
    graph.endTime = 0;
    graph.factor = 1;
    graph.factorList = [1];
    graph.timeFormat = d3.time.format("%m/%d-%H:%M:%S");

    graphRangeInit(graph);
    graphFactorInit(graph);
}

function resetGraph(graph) {
    d3.selectAll("#graph-area").remove();
    graph.svg = null;
    graph.legend = null;

    graph.svg = d3.select("#graph").append("svg:svg")
                                       .attr("id", "graph-area")
                                       .attr("width", graph.width + graph.margin.left + graph.margin.right)
                                       .attr("height", graph.height + graph.margin.top + graph.margin.bottom)
                                   .append("svg:g")
                                       .attr("transform", "translate(" + graph.margin.left + "," + graph.margin.top + ")");
    graph.legend = graph.svg.append("svg:g").attr("class", "legend");
}

function addToGraphData(graph, graphData, data) {
    graph.editInProgress = true;

    if(!(graph.factorList.indexOf(Number(data.factor)) >= 0)) {
        graph.factorList.push(Number(data.factor));
        graph.factorList.sort(function(a, b) {
                                  return a - b;
                              });
    }

    if(!graphData["factors"].hasOwnProperty(data.factor)) {
        graphData["factors"][data.factor] = {"mintime": null,
                                             "maxtime": null,
                                             "maxoffset": null,
                                             "names": {}};
    }

    if(!graphData["factors"][data.factor]["names"].hasOwnProperty(data.name)) {
        graphData["factors"][data.factor]["names"][data.name] = {"mintime": null,
                                                                 "maxtime": null,
                                                                 "maxoffset": null,
                                                                 "data": []};
    }

    graphData["factors"][data.factor]["names"][data.name]["data"].push([data.timestamp, data.offset]);

    adjustLast(graphData["factors"][data.factor]["names"][data.name]);
    checkFactorExtents(graphData["factors"][data.factor], data.name);
    checkDataExtents(graphData, data.factor);
    graph.editInProgress = false;
    graph.fullyUpdated = false;
}

function adjustLast(data) {
    var index = data["data"].length - 1;

    data["data"][index][1] = parseInt(data["data"][index][1]);
    data["data"][index][0] = new Date(parseInt(data["data"][index][0]));

    if((data["data"][index][0] < data.mintime)   || (data.mintime == null))   { data.mintime =   data["data"][index][0]; }
    if((data["data"][index][0] > data.maxtime)   || (data.maxtime == null))   { data.maxtime =   data["data"][index][0]; }
    if((data["data"][index][1] > data.maxoffset) || (data.maxoffset == null)) { data.maxoffset = data["data"][index][1]; }
}

function checkFactorExtents(factor, name) {
    if((factor["names"][name].mintime   < factor.mintime)   || (factor.mintime == null))   { factor.mintime   = factor["names"][name].mintime; }
    if((factor["names"][name].maxtime   > factor.maxtime)   || (factor.maxtime == null))   { factor.maxtime   = factor["names"][name].maxtime; }
    if((factor["names"][name].maxoffset > factor.maxoffset) || (factor.maxoffset == null)) { factor.maxoffset = factor["names"][name].maxoffset; }
}

function checkDataExtents(graphData, factor) {
    if((graphData["factors"][factor].mintime   < graphData.mintime)   || (graphData.mintime == null))   { graphData.mintime   = graphData["factors"][factor].mintime; }
    if((graphData["factors"][factor].maxtime   > graphData.maxtime)   || (graphData.maxtime == null))   { graphData.maxtime   = graphData["factors"][factor].maxtime; }
    if((graphData["factors"][factor].maxoffset > graphData.maxoffset) || (graphData.maxoffset == null)) { graphData.maxoffset = graphData["factors"][factor].maxoffset; }
}

// initGraph(graph) MUST be called before the first drawing pass
function drawGraph(graph, graphData) {
    if(!graph.fullyUpdated && !graph.editInProgress) {
        graph.fullyUpdated = true;

        graphRangeUpdate(graphData.mintime,
                          graphData.maxtime);
        graphFactorUpdate(graph.factorList);

        resetGraph(graph);

        graph.color.domain(d3.keys(graphData["factors"][graph.factor]["names"]).sort());

        graph.x.domain([graph.beginTime, graph.endTime]);
        graph.y.domain([0, graphData.maxoffset]);

        graph.svg.append("svg:g").attr("class", "x axis")
                       .attr("transform", "translate(0," + graph.height + ")")
                       .call(graph.xAxis);
        graph.svg.append("svg:g").attr("class", "y axis")
                       .call(graph.yAxis)
           .append("svg:text").attr("transform", "translate(-65,0) rotate(-90)")
                       .attr("y", 6)
                       .attr("dy", ".71em")
                       .style("text-anchor", "end")
                       .text("Throughput (messages/second)");

        var h = 20;
        var names = d3.keys(graphData["factors"][graph.factor]["names"]).sort();
        for(var i = 0; i < names.length; i++) {
            var name = names[i];

            var data = removeExtents(graphData["factors"][graph.factor]["names"][name]["data"], graph.beginTime, graph.endTime);

            graph.svg.append("svg:path").style("stroke", graph.color(name))
                                  .attr("class", "line")
                                  .attr("d", graph.line(data));
            graph.legend.append("rect").attr("x", graph.width + 10).attr("y", h).attr("width", 10).attr("height", 10).style("fill", graph.color(name));
            graph.legend.append("text").attr("x", graph.width + 25).attr("y", h + 10).text(name);
            h = h + 20;
        }
    }
}

function removeExtents(data, beginTime, endTime) {
    var beginIndex = 0;
    var endIndex = data.length - 1;
    for(var i = 0; i < data.length; i++) {
        if(data[i][0] <= beginTime) {
            beginIndex = i;
        } else if(data[i][0] <= endTime) {
            endIndex = i;
        }
    }
    return data.slice(beginIndex+1, endIndex);
}