#!/usr/bin/env node --max-old-space-size=12192
// node merge.js out.json in1.json in2.json ...

'use strict';

var fs      = require('fs');

var debug   = require('debug')('ff');
var _       = require('underscore');


var OUT = process.argv[2];
var FILES = process.argv.slice(3);



console.log('IN:', FILES);
console.log('OUT:', OUT);


var data = FILES.map(function (name) {
    debug('loading', name);
    var accounts = JSON.parse(fs.readFileSync(name, 'utf8'));
    debug('loaded', _.keys(accounts).length);
    //debug(_.keys(accounts));
    return {name: name, accounts: accounts};
});

var merged = {};
data.forEach(function (dict) {
    debug('merging', dict.name);
    for (var name in dict.accounts) {
        if (!merged[name]) {
            merged[name] = {};
        }
        var account = merged[name];
        var fresh = dict.accounts[name];
        for (var fld in fresh) {
            account[fld] = fresh[fld];
        }
    }
});


debug('counting nodes');
var degrees = {};
for (var name in merged) {
    var act = merged[name];
    if (act.nfo) {
        degrees[act.nfo.id] = 2;
    }
    if (act.followers) {
        for (var i = 0; i < act.followers.length; i++) {
            var id = act.followers[i];
            degrees[id] = (degrees[id] || 0) + 1;
        }
    }
}
debug('counted accts', _.keys(merged).length);
var highDegree = 0;
for (var i in degrees) {
    if (degrees[i] > 1) {
        highDegree++;
    }
}
debug('counted ids', _.keys(degrees).length, 'used', highDegree);

debug('trimming supernodes')
for (var name in merged) {
    var act = merged[name];
    var SAMPLE = 100000000;
    if (act.followers && act.followers.length > SAMPLE * 10) {
        var ok = [];
        for (var i = 0; i < SAMPLE; i++) {
            ok.push(act.followers[i]);
        }
        for (var i = SAMPLE; i < act.followers.length; i++) {
            var id = act.followers[i];
            if (degrees[id] > 1) {
                ok.push(i);
            }
        }
        act.followers = ok;
    }
    act.nfo =
        act.nfo ?
            _.object(
            ['created_at', 'description', 'distance',
             'favourites_count', 'followers_count', 'friends_count',
             'geo_enabled', 'id', 'name', 'screen_name',
             'statuses_count', 'url', 'verified',
             'profile_image_url', 'profile_image_url_https']
            .map(function (fld) {
                return [fld, act.nfo[fld]]
            }))
            : undefined;
}


debug('loaded');


function save(filename, accounts) {
    var t0 = Date.now();
    console.log('Saving data as', OUT, '( #accounts:', _.keys(accounts).length, ')');
    fs.writeFileSync(filename, JSON.stringify(accounts));
    console.log('Done saving', Date.now() - t0, 'ms');
}

save(OUT, merged);
