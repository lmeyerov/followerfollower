#!/usr/bin/env node --max-old-space-size=8192

'use strict';

var fs  = require('fs');
var cfg = JSON.parse(fs.readFileSync('config.json', 'utf8'));

var debug   = require('debug')('ff');
var Twitter = require('twitter');
var _       = require('underscore');

var MAX_RET = cfg.MAX_RET || 100000;
var SEEDS   = cfg.SEEDS || ['lmeyerov'];
var FILE_NAME = cfg.FILE_NAME || 'accounts.json'



//==========

//{url -> {remaining: int, reset: next ok utc time in seconds}}
var limits = {};

//{name -> {followers: [id], nfo: {...}}}
//(persisted in accounts.json)
var accounts = {};

//{id -> account}
var idToAccount = {};

//{id -> int}
var idToDegree = {};

//{id -> int}
var idToDistance = {};

var client = new Twitter(cfg);

var blacklistIds = {};

var exiting = false;

//===========

if (fs.existsSync(FILE_NAME)) {
    console.log('reloading last session: followed users and follower ids');
    var raw = fs.readFileSync(FILE_NAME);
    accounts = JSON.parse(raw);
    debug('reloaded', _.keys(accounts).length, 'accounts');
    raw = null;
}

for (var name in accounts) {
    var account =  accounts[name];
    var id = account.nfo.id;
    var distance = account.nfo.distance;
    idToAccount[id] = account;
    idToDistance[id] = distance;

    if (distance === undefined) {
        throw new Error('bad nfo distance');
    }

    if (account.followers) {
        for (var i = 0; i < account.followers.length; i++) {
            var followerID = account.followers[i];
            idToDistance[followerID] =
                Math.min(distance + 1,
                    idToDistance[followerID] !== undefined ?
                        idToDistance[followerID] : (distance + 1));
            idToDistance[followerID] = (idToDistance[followerID] || 0) + 1;
        }
    }
}

function save(filename) {
    var t0 = Date.now();
    console.log('Saving data as', filename, '( #accounts:', _.keys(accounts).length, ')');
    fs.writeFileSync(filename, JSON.stringify(accounts));
    console.log('Done saving', Date.now() - t0, 'ms');
}

function addLookup(user, maybeK) {
    if (user.distance === undefined) {
        throw new Error('missing distance');
    }

    if (!accounts[user.screen_name]) {
        accounts[user.screen_name] = {};
    }
    var account = accounts[user.screen_name];
    idToAccount[user.id] = account;
    idToDistance[user.id] = user.distance;
    account.nfo = user;

    (maybeK || function () {})();
}

function addFollowers(id, ids, maybeK) {
    var account = idToAccount[id];
    if (!account) { return (maybeK || function (){})('addFollowers: missing account ' + name); }

    account.followers = _.union(account.followers || [], ids);
    var distance = idToDistance[id];
    for (var i = 0; i < account.followers.length; i++) {
        var followerID = account.followers[i];
        idToAccount[followerID] = {};
        idToDistance[followerID] =
            Math.min(distance + 1,
                idToDistance[followerID] !== undefined ?
                    idToDistance[followerID] : (distance + 1));
        idToDegree[followerID] = (idToDegree[followerID] || 0) + 1;
    }

    (maybeK || function () {})();
}

//==========

function updateLimits (k) {
    debug('updating limits');
    client.get('/application/rate_limit_status', function (err, res, resp) {
        if (err) { return k({msg: 'bad limits', err: err}); }
        else {
            debug('got rate limits');
            limits = {};
            for (var mode in res.resources) {
                for (var path in res.resources[mode]) {
                    limits[path] = res.resources[mode][path];
                }
            }

            var listLimit = limits['/followers/ids'];
            debug('expand left', listLimit, ((listLimit.reset - (Date.now() / 1000))/60).toFixed(1),'min');

            var lookupLimit = limits['/users/lookup'];
            debug('annotate left', lookupLimit, ((lookupLimit.reset - (Date.now() / 1000))/60).toFixed(1),'min');

            return k(null, limits);
        }
    });
}


function onReady (cmd, k) {
    debug('checking cmd', cmd);
    if (limits[cmd].remaining) {
        limits[cmd].remaining--;
        k();
    } else {
        var when = 1000 * ((limits[cmd].reset - (Date.now()/1000)) + 1);
        debug('defer', cmd, ((when/1000)/60).toFixed(1), 'min', limits[cmd]);
        setTimeout(
            function () {
                updateLimits(function (err) {
                    if (err) { return k(err); }
                    debug('resuming', cmd);
                    k();
                });
            },
            when);
    }

}

//=================

//[ {id: *, distance: int}] * cb -> ()
function annotateIds (pairs, k) {
    var origCount = pairs.length;
    debug('annotateIds', pairs.length);

    var cmd = '/users/lookup';
    onReady(cmd, function (err) {
        if (err) { return k(err); }

        debug('Maybe add more IDs, already has', pairs.length);
        if (pairs.length < 100) {
            var attempts = 10000;
            var opts = _.keys(idToAccount);
            while (pairs.length < 100 && attempts--) {
                var idx = Math.round(Math.random() * (opts.length - 1));
                var id = opts[idx];
                if ((!idToAccount[id] || !idToAccount[id].nfo) && !blacklistIds[id]) {
                    pairs.push({id: id, distance: idToDistance[id]});
                }
            }
        }

        debug('annotating IDs',
            _.pluck(pairs,'id').slice(0, origCount).slice(0,10).join(',') + '..',
            pairs.length);
        client.get(cmd, {user_id: _.pluck(pairs,'id').join(',')},
            function (err, annotations) {
                debug('downloaded annotations');
                if (err) {
                    pairs.forEach(function (pair) {
                        blacklistIds[pair.id] = true;
                    });
                    return k(err);
                }
                var done = 0;
                var errors;
                annotations.forEach(function (nfo) {
                    nfo.distance = pairs.filter(function (pair) { return ('' + pair.id) === ('' + nfo.id); })[0].distance;
                    if ((idToDistance[nfo.id] !== undefined) && idToDistance[nfo.id] < nfo.distance) {
                        nfo.distance = idToDistance[nfo.id];
                    }
                    addLookup(nfo, function (err) {
                        done++;
                        errors = errors || err;
                        if (err) {
                            blacklistIds[nfo.id] = true;
                        }
                        if (done == annotations.length) {
                            debug('done annotating, checking');
                            pairs.forEach(function (pair) {
                                if (!idToAccount[pair.id] || !idToAccount[pair.id].nfo) {
                                    blacklistIds[pair.id] = true;
                                }
                            });
                            return k(errors);
                        }
                    });
                });
            });
    });
}

//[{name: string, distance: int}] * cb -> ()
function annotateNames (pairs, k) {
    if (!pairs.length) {
        return k();
    }

    var cmd = '/users/lookup';
    onReady(cmd, function (err) {
        if (err) { return k(err); }

        debug('annotating names', _.pluck(pairs, 'name'), pairs.length);
        client.get(cmd, {screen_name: _.pluck(pairs, 'name').join(',')},
            function (err, annotations) {
                if (err) { return k(err); }
                var doneCount = 0;
                var errors;
                annotations.forEach(function (nfo) {
                    var distance =
                        pairs
                            .filter(function (o) { return o.name == nfo.screen_name; })[0]
                            .distance;
                    nfo.distance = distance;
                    if ((idToDistance[nfo.id] !== undefined) && idToDistance[nfo.id] < nfo.distance) {
                        nfo.distance = idToDistance[nfo.id];
                    }
                    addLookup(nfo, function (err) {
                        doneCount++;
                        errors = errors || err;
                        if (doneCount == annotations.length) {
                            return k(errors);
                        }
                    });
                });
            });
    });
}

function followers (id, k) {

    var cmd = '/followers/ids';
    var who = {user_id: id, cursor: -1, count: 5000};

    onReady(cmd, function (err) {
        if (err) { return k(err); }

        debug('fetching', id, idToAccount[id].nfo.screen_name);
        client.get(cmd, who,
            function (err, ids, resp) {
                if (err) {
                    debug('bad fetch', id);
                    blacklistIds[id] = true;
                    idToAccount[id].followers = [];
                    return k(err);
                }
                debug('fetched', id, ids.length, ids.ids.slice(0,10) + '...');
                addFollowers(id, ids.ids, k);
            });
    });
}


//====================

function addAnnotations () {

    try {
        var cmd = '/users/lookup';

        debug('annotate poller');
        if (limits[cmd].remaining > 17) {
            var incomplete =
                _.keys(idToAccount)
                    .filter(function (id) { return !idToAccount[id] || !idToAccount[id].nfo; })
                    .slice(0, 100)
                    .map(function (id) {
                        return {id: id, distance: idToDistance[id]};
                    });
            debug('computed incomplete', incomplete.length);
            if (incomplete.length > 50) {
                return annotateIds(incomplete, function (err) {
                    if (err) {
                        console.error('error polling annotated', err);
                    } else {
                        debug('annotated extra names');
                    }
                    debug('pause');
                    return setTimeout(addAnnotations, (err ? 30 : 1) * 1000);
                });
            } else {
                console.warn('not enough names for poller, wait for more expansions');
                debug('pause');
                return setTimeout(addAnnotations, 30 * 1000);
            }
        } else {
            var when = 1000 * ((limits[cmd].reset - (Date.now()/1000)) + 1);
            debug('not enough extras to annotate while allowing explore so wait', ((when/1000)/60).toFixed(1), 'min');
            debug('pause');
            return setTimeout(addAnnotations, Math.max(when, 30 * 1000));
        }
    } catch (e) {
        debug('exn adding annotations, restart', e);
        setTimeout(addAnnotations, 3 * 1000);
    }

}
//====================


// [ {id: *, distance: int}] * int * cb -> ()
function explore (seeds, amt, k) {

    debug('explore call');

    if (!amt) { return k(); }

    var pair;
    var distance;
    if (seeds.length) {
        pair = seeds.pop();
    }
    if (pair === undefined) {

        //coin flip between an existing connected but unexplored node, random dfs, & bfs

        debug('presort: _.keys');
        var ids = _.keys(idToAccount);

        var flip = Math.random();
        if (flip > 0.5) {
            debug('popular unexplored');
            ids.sort(function (a, b) { return idToDegree[b] - idToDegree[a]; });
            for (var i = 0; i < ids.length; i++) {
                var id = ids[i];
                if ((!idToAccount[id] || !idToAccount[id].followers) && !blacklistIds[id]) {
                    pair = {id: id, distance: idToDistance[id]};
                    break;
                }
            }
        } else if (flip > 0.25) {
            debug('sorting');
            ids.sort(function (a, b) { return idToDistance[a] - idToDistance[b]; });
            debug('exploring dfs');

            //personalized pagerank, to avoid letting a supernode dominate
            var id = ids[0];
            var dist = 0;
            var max = 1000;
            while (idToAccount[id] && idToAccount[id].followers) {
                if (!idToAccount[id].followers.length) {
                    debug('endpoint, retry');
                    return explore(seeds, amt, k);
                }
                id = idToAccount[id].followers[
                    Math.round(Math.random() * (idToAccount[id].followers.length - 1))];
                dist++;
                if (blacklistIds[id]) {
                    if (max--) {
                        debug('blacklisted, retry', id);
                        id = ids[0];
                        dist = 0;
                    } else {
                        debug('blacklisted, full retry');
                        return explore(seeds, amt, k);
                    }
                }
            }
            pair = {id: id, distance: dist};
        } else {
            debug('sorting');
            ids.sort(function (a, b) { return idToDistance[a] - idToDistance[b]; });
            debug('exploring bfs');
            for (var i = 0; i < ids.length; i++) {
                var id = ids[i];
                if ((!idToAccount[id] || !idToAccount[id].followers) && !blacklistIds[id]) {
                    pair = {id: id, distance: idToDistance[id]};
                    break;
                }
            }
        }
        debug('picked pair', pair);
        if (!pair) { return k('exhausted'); }
        if (blacklistIds[pair.id]) {
            debug('picked blacklisted, retry');
            return explore(seeds, amt, k);
        }
    }

    var proceed = function (err) {
        if (err) { return k(err); }
        if (!idToAccount[pair.id] || !idToAccount[pair.id].nfo) {
            console.error('failed to lookup before expanding, try another', pair.id);
            delete idToAccount[pair.id];
            blacklistIds[pair.id] = true;
            return explore(seeds, amt, k);
        }
        debug('===================== expanding', pair.id, idToAccount[pair.id].nfo.screen_name, 'dist:', idToDistance[pair.id]);
        followers(
            pair.id,
            function (err) {
                if (err) {
                    console.error('~~~~ignoring expand err, skipping', pair.id, idToAccount[pair.id].nfo.screen_name, err);
                    delete accounts[idToAccount[pair.id].nfo.screen_name];
                    delete idToAccount[pair.id];
                    blacklistIds[pair.id] = true;
                } else {
                    debug('expanded', pair.id, idToAccount[pair.id].nfo.screen_name, 'followers', idToAccount[pair.id].followers.length, 'distance', idToDistance[pair.id]);
                }
                explore(seeds, err ? amt : (amt - 1) , k);
        });
    };

    if (!idToAccount[pair.id] || !idToAccount[pair.id].nfo) {
        debug('~~~expanding but first annotating', pair.id);
        annotateIds([pair], proceed);
    } else {
        proceed();
    }
}

function crawler (seeds, amt, k) {

    debug('crawling with seeds', seeds);

    var missingSeeds = seeds.filter(function (name) { return !accounts[name]; });
    if (missingSeeds.length) { debug('first annotate', missingSeeds); }
    annotateNames(
        missingSeeds.map(function (name) { return {name: name, distance: 0}; }),
        function (err) {
            if (err) { return k(err); }

            if (missingSeeds.length) { debug('annotated missing'); }

            debug('start exploring');
            explore(
                seeds
                    .filter(function (name) { return accounts[name] && !accounts[name].followers; })
                    .map(function (name) {
                        return {id: accounts[name].nfo.id, distance: accounts[name].nfo.distance};
                    })
                    .filter(function (pair) { return !blacklistIds[pair.id];  }),
                amt - 1,
                k);
        });
}


function crawl () {
     crawler(SEEDS, MAX_RET, function (err, network) {
        if (err) {
            console.error('error', err);
            save(FILE_NAME + '.tmp.' + (Math.round(Math.random()) * 100000));
            debug('RESTARTING');
            crawl();
            //process.exit(-1);
        } else {
            debug(
                'done',
                _.keys(accounts)
                    .filter(function (name) { return accounts[name].followers; }));
        }
    });
}



function go () {
    updateLimits(function (err, limits) {
        if (err) {
            console.error('bad limits', err);
            debug('restarting');
            go();
        }

        addAnnotations();

        crawl();

    });
}

process.on('SIGINT', function () {
    if (!exiting) {
        console.log('Got SIGINT, exiting...');
        exiting = true;
        save(FILE_NAME);
        process.exit(0);
    }
});

setInterval(save.bind('', FILE_NAME), 15 * 60 * 1000);

go();

