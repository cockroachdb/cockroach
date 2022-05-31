#!/usr/bin/env node
// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.


const fs = require('fs');
const path = require('path');

const d3geo = require('d3-geo');
const d3geoProjection = require('d3-geo-projection');
const topojson = require('topojson');

const usOrig = require('../node_modules/us-atlas/us/10m.json');
const worldOrig = require('../node_modules/world-atlas/world/50m.json');

const projection = d3geo.geoAlbersUsa().scale(1280).translate([480, 300]);
const invert = d3geo.geoTransform({
    point: function(x, y) {
        const inverted = projection.invert([x, y]);
        this.stream.point(inverted[0], inverted[1]);
    }
});

const usFeatProjected = topojson.feature(usOrig, {
    type: "GeometryCollection",
    geometries: usOrig.objects.states.geometries,
});
const usFeat = d3geoProjection.geoProject(usFeatProjected, invert);
const worldFeat = topojson.feature(worldOrig, {
    type: "GeometryCollection",
    geometries: worldOrig.objects.countries.geometries.filter(c => c.id != "840"),
});

const combinedFeats = {
    type: "FeatureCollection",
    features: usFeat.features.concat(worldFeat.features),
};

const combinedTopo = topojson.topology({all: combinedFeats}, 1e3);

let combinedSimpl = topojson.presimplify(combinedTopo);
combinedSimpl = topojson.simplify(combinedSimpl, 0.25);

const combinedResult = topojson.feature(combinedSimpl, combinedSimpl.objects.all);
const combinedSerialized = JSON.stringify(combinedResult);

const outfile = path.join(__dirname, '..', 'ccl', 'src', 'views', 'clusterviz', 'containers', 'map', 'world.json');
fs.writeFileSync(outfile, combinedSerialized);
