#!/usr/bin/env node --max-old-space-size=8192
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

debug('loaded');


function save(filename, accounts) {
    var t0 = Date.now();
    console.log('Saving data as', OUT, '( #accounts:', _.keys(accounts).length, ')');
    fs.writeFileSync(filename, JSON.stringify(accounts));
    console.log('Done saving', Date.now() - t0, 'ms');
}

save(OUT, merged);
