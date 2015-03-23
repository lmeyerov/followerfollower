var fs  = require('fs');
var cfg = JSON.parse(fs.readFileSync('config.json', 'utf8'));

var debug   = require('debug')('ff');
var Twitter = require('twitter');
var _       = require('underscore');
var Store   = require('jfs');

var MAX_RET = 3;
var SEEDS   = ['lmeyerov'];


//==========

//{url -> {remaining: int, reset: next ok utc time in seconds}}
var limits = {};

//{name -> {followers: [id], nfo: {...}}}
//(persisted in accounts.json)
var accounts = {};

//relevant user ids, bool indicates whether already followed
//{id -> bool}
var userIds = {};

//{id -> account}
var idToAccount = {};

//{id -> int}
var idToDistance = {};

var client = new Twitter(cfg);

var blacklistIds = {};

var db = new Store('./accounts.json', {saveId: 'screen_name'});

//===========

debug('reloading last session: followed users and follower ids');
var objs = db.allSync();
for (var name in objs) {
    var account =  objs[name];
    var id = account.nfo.id;
    var distance = account.nfo.distance;
    accounts[name] = account;
    userIds[id] = true;
    idToAccount[id] = account;
    idToDistance[id] = distance;

    if (distance === undefined) {
        throw new Error('bad nfo distance');
    }

    if (account.followers) {
        for (var i = 0; i < account.followers.length; i++) {
            var followerID = account.followers[i];
            userIds[followerID] = userIds[followerID] || false;
            idToDistance[followerID] =
                Math.min(distance + 1,
                    idToDistance[followerID] !== undefined ?
                        idToDistance[followerID] : (distance + 1));
        }
    }
}
debug('reloaded', _.keys(objs).length, 'followed');

function saveLookup(user, maybeK) {
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
    userIds[user.id] = userIds[user.id] || false;

    db.save(
        user.screen_name, account,
        maybeK || function (err) { if (err) { console.error('Error onsaveLookup', err); } });
}

function saveFollowers(id, ids) {
    var account = idToAccount[id];
    if (!account) { return k('saveFollowers: missing account ' + name); }

    account.followers = _.union(account.followers || [], ids);
    var distance = idToDistance[id];
    for (var i = 0; i < account.followers.length; i++) {
        var followerID = account.followers[i];
        userIds[followerID] = userIds[followerID] || false;
        idToDistance[followerID] =
            Math.min(distance + 1,
                idToDistance[followerID] !== undefined ?
                    idToDistance[followerID] : (distance + 1));
    }
    userIds[id] = true;

    db.saveSync(account.nfo.screen_name, account);
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

    var cmd = '/users/lookup';
    onReady(cmd, function (err) {
        if (err) { return k(err); }

        //lookup more, while we're add it
        if (pairs.length < 100) {
            var attempts = 10000;
            while (pairs.length < 100 && attempts--) {
                var opts = _.keys(userIds);
                var idx = Math.round(Math.random() * (opts.length - 1));
                var id = opts[idx];
                if (!idToAccount[id] || !idToAccount[id].nfo) {
                    pairs.push({id: id, distance: idToDistance[id]});
                }
            }
        }

        debug('annotating IDs',
            _.pluck(pairs,'id').slice(0, origCount).slice(0,10).join(',') + '..',
            pairs.length);
        client.get(cmd, {user_id: _.pluck(pairs,'id').join(',')},
            function (err, annotations) {
                if (err) { return k(err); }
                var done = 0;
                var errors;
                annotations.forEach(function (nfo) {
                    nfo.distance = pairs.filter(function (pair) { return ('' + pair.id) === ('' + nfo.id); })[0].distance;
                    if ((idToDistance[nfo.id] !== undefined) && idToDistance[nfo.id] < nfo.distance) {
                        nfo.distance = idToDistance[nfo.id];
                    }
                    saveLookup(nfo, function (err) {
                        done++;
                        errors = errors || err;
                        if (done == annotations.length) {
                            debug('done annotating');
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
                    saveLookup(nfo, function (err) {
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
                if (err) {return k(err); }
                debug('fetched', id, ids.ids.slice(0,10) + '...');
                saveFollowers(id, ids.ids);
                k();
            });
    });
}


//====================

function addAnnotations () {

    var cmd = '/users/lookup';

    var annotate = function () {

        debug('annotate poller');
        if (limits[cmd].remaining > 20) {
            var incomplete =
                _.keys(userIds)
                    .filter(function (id) { return !idToAccount[id] || !idToAccount[id].nfo; })
                    .slice(0, 100)
                    .map(function (id) {
                        return {id: id, distance: idToDistance[id]};
                    });
            if (incomplete.length > 50) {
                return annotateIds(incomplete, function (err) {
                    if (err) {
                        console.error('error polling annotated', err);
                    } else {
                        debug('annotated extra names');
                    }
                    return setTimeout(annotate, (err ? 30 :3) * 1000);
                });
            } else {
                console.warn('not enough names, wait for more expansions', err);
                return setTimeout(annotate, 30 * 1000);
            }
        } else {
            var when = 1000 * ((limits[cmd].reset - (Date.now()/1000)) + 1);
            debug('not enough extras to annotate', ((when/1000)/60).toFixed(1), 'min');
            return setTimeout(annotate, Math.max(when, 30 * 1000));
        }

    };

    annotate();

}
//====================


// [ {id: *, distance: int}] * int * cb -> ()
function explore (seeds, amt, k) {

    debug('explore call');

    var pair;
    var distance;
    if (seeds.length) {
        pair = seeds.pop();
    }
    if (pair === undefined) {
        //coin flip between random & bfs

        var ids = _.keys(userIds);
        ids.sort(function (a, b) { return idToDistance[a] - idToDistance[b]; });

        if (Math.random() > 0.8) {
            debug('exploring dfs');
            //personalized pagerank, to avoid letting a supernode dominate
            var id = ids[0];
            var dist = 0;
            while (idToAccount[id] && idToAccount[id].followers) {
                id = idToAccount[id].followers[
                    Math.round(Math.random() * (idToAccount[id].followers.length - 1))];
                dist++;
            }
            pair = {id: id, distance: dist};
        } else {
            debug('exploring bfs');
            for (var i = 0; i < ids.length; i++) {
                var id = ids[i];
                if ((!idToAccount[id] || !idToAccount[id].followers) && !blacklistIds[id]) {
                    pair = {id: id, distance: idToDistance[id]};
                    break;
                }
            }
        }
        if (!pair) { return k('exhausted'); }
    }

    var proceed = function (err) {
        if (err) { return k(err); }
        if (!idToAccount[pair.id]) {
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
                    }),
                amt - 1,
                k);
        });
}


function go () {
    updateLimits(function (err, limits) {
        if (err) { return console.error('bad limits', err); }

        addAnnotations();

        crawler(SEEDS, MAX_RET, function (err, network) {
            if (err) {
                console.error('error', err);
                debug('RESTARTING');
                go();
            }
            else {
                debug(
                    'done',
                    _.keys(accounts)
                        .filter(function (name) { return accounts[name].followers; }));
                }
        });
    });
}

go();

