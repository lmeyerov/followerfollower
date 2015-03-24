#!/usr/bin/env node --max-old-space-size=8192

'use strict';

var fs  = require('fs');

var request = require('request');
var _       = require('underscore');
var debug   = require('debug')('ff');

var cfg = JSON.parse(fs.readFileSync('config.json', 'utf8'));
var DATASET_NAME = cfg.DATASET_NAME || ("TwitterV" + Math.round(Math.random() * 10000000));
debug('loaded cfg', cfg);

var args = process.argv.slice(2);
if (args.length !== 1) {
    console.log('need filename to upload');
    process.exit(1);
}
var filename = args[0];

var accounts = JSON.parse(fs.readFileSync(filename, 'utf8'));
debug ('loaded file', filename);

var state = {
    edges: [],
    nodes: []
};

var expandedNodes = {};
_.values(accounts).forEach(function (account) {
    if (account.nfo) {
        account.nfo.node = account.nfo.id;
        account.nfo.pointTitle = account.nfo.screen_name || ('id:' + account.nfo.id);
        expandedNodes[account.nfo.id] = true;
        state.nodes.push(account.nfo);
    }
});
debug('loaded expanded accounts', state.nodes.length);
function maybeAddDummy (id) {
    if (!expandedNodes[id]) {
        state.nodes.push({node: id, pointTitle: 'id:' + id});
        expandedNodes[id] = true;
    }
}
_.values(accounts).forEach(function (account) {
    var followers = account.followers || [];
    var nfo = account.nfo;
    var id = nfo.id;
    maybeAddDummy(id);
    for (var i = 0; i < followers.length; i++) {
        var follower = followers[i];
        state.edges.push({src: follower, dst: id});
        maybeAddDummy(follower);
    }
});
debug('loaded unexpanded accounts', state.nodes.length);


function upload (data) {
    request.post('http://localhost:3000/etl',
        {form: JSON.stringify(data)},
        function (err, resp, body) {
            debug('uploaded');
            if (err) {
                return console.error('nooo', err);
            }

            if (!JSON.parse(body).success) {
                return console.error('upload fail', body);
            }

            console.log('OK!', body);
            console.log('nodes:', data.labels.length);
            console.log('edges:', data.graph.length);
        });
}

function bundle () {
    return {
        name: DATASET_NAME,
        type: 'edgelist',
        graph: state.edges,
        labels: state.nodes,
        bindings: {
            sourceField: 'src',
            destinationField: 'dst',
            idField: 'node'
        }
    };
}

var data = bundle();
debug('bundled');

upload(data);
