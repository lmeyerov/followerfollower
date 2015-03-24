#!/usr/bin/env node

'use strict';

var fs  = require('fs');

var request = require('request');
var _       = require('underscore');

var FILE_NAME = 'Twitter3';
var accounts = JSON.parse(fs.readFileSync('accounts.json', 'utf8'));


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


function upload (data) {
    request.post('http://localhost:3000/etl',
        {form: JSON.stringify(data)},
        function (err, resp, body) {
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
        name: FILE_NAME,
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

upload(bundle());
