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

function graphRangeInit(graph) {
    $("#slider-range").slider({
        range: true,
        min: 0,
        max: 0,
        change: function(event, ui) {
            $("#range").val(graph.timeFormat(new Date(ui.values[0])) + " - " + graph.timeFormat(new Date(ui.values[1])));
            graph.beginTime = new Date(ui.values[0]);
            graph.endTime   = new Date(ui.values[1]);
            graph.fullyUpdated = false;
        },
        slide: function(event, ui) {
            $("#range").val(graph.timeFormat(new Date(ui.values[0])) + " - " + graph.timeFormat(new Date(ui.values[1])));
            graph.beginTime = new Date(ui.values[0]);
            graph.endTime   = new Date(ui.values[1]);
            graph.fullyUpdated = false;
        }
    });
    $("#range").val(graph.timeFormat(new Date($("#slider-range").slider("values", 0))) + " - " + graph.timeFormat(new Date($("#slider-range").slider("values", 1))));
}

function graphRangeUpdate(min, max) {
    var slider = $("#slider-range");
    var preMin = slider.slider("option", "min");
    var preMax = slider.slider("option", "max");
    var valMin = slider.slider("values", 0);
    var valMax = slider.slider("values", 1);

    if(preMin == valMin) {
        valMin = min;
    }
    if(preMax == valMax) {
        valMax = max;
    }

    slider.slider("option", "min", Number(min));
    slider.slider("option", "max", Number(max));
    slider.slider("option", "values", [Number(valMin), Number(valMax)]);
}

function graphFactorInit(graph) {
    $("#slider-factor").slider({
      min: 0,
      max: 0,
      change: function(event, ui) {
        $("#factor").val(graph.factorList[ui.value] + "x");
        graph.factor = graph.factorList[ui.value];
        graph.fullyUpdated = false;
      },
      slide: function(event, ui) {
        $("#factor").val(graph.factorList[ui.value] + "x");
        graph.factor = graph.factorList[ui.value];
        graph.fullyUpdated = false;
      }
    });
    $("#factor").val(graph.factorList[$("#slider-factor").slider("option", "value")] + "x");
}

function graphFactorUpdate(factorList) {
    $("#slider-factor").slider("option", "max", (factorList.length - 1));
}