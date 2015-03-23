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

var client = new Twitter(cfg);

var db = new Store('./accounts.json', {saveId: 'screen_name'});

//===========

debug('reloading last session: followed users and follower ids');
var objs = db.allSync();
for (var name in objs) {
    var account =  objs[name];
    accounts[name] = account;
    userIds[account.id] = true;
    idToAccount[account.id] = account;
    if (account.followers) {
        for (var i = 0; i < account.followers.length; i++) {
            userIds[account.followers[i]] = userIds[account.followers[i]] || false;
        }
    }
}
debug('reloaded', _.keys(objs).length, 'followed');


function saveLookup(user) {
    if (!accounts[user.screen_name]) {
        accounts[user.screen_name] = {};
    }
    var account = accounts[user.screen_name];
    idToAccount[user.id] = account;
    account.nfo = user;
    userIds[user.id] = userIds[user.id] || false;

    db.saveSync(user.screen_name, account);
}

function saveFollowers(id, ids) {
    var account = idToAccount[id];
    if (!account) { return k('saveFollowers: missing account ' + name); }

    account.followers = _.union(account.followers || [], ids);
    for (var i = 0; i < account.followers.length; i++) {
        userIds[account.followers[i]] = userIds[account.followers[i]] || false;
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
            var listLimit = limits['/followers/list'];
            debug('list left', listLimit, listLimit.reset - (Date.now() / 1000),'ms');
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
        debug('defer', cmd, ((when/1000)/60), 'min', limits[cmd]);
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

function annotateIds (ids, k) {
    var origCount = ids.length;

    var cmd = '/users/lookup';
    onReady(cmd, function (err) {
        if (err) { return k(err); }

        var attempts = 10000;
        if (ids.length < 100 && attempts--) {
            var opts = _.keys(userIds);
            var idx = Math.round(Math.random() * (opts.length - 1));
            var id = opts[idx];
            if (!idToAccount[id] || !idToAccount[id].nfo) {
                ids.push(id);
            }
        }

        debug('annotating', ids.slice(0, origCount).slice(0,10).join(',') + '..', ids.length);
        client.get(cmd, {user_id: ids.join(',')},
            function (err, annotations) {
                if (err) { return k(err); }
                annotations.forEach(function (nfo) {
                    saveLookup(nfo);
                });
                return k();
            });
    });
}

function annotateNames (names, k) {
    if (!names.length) {
        return k();
    }

    var cmd = '/users/lookup';
    onReady(cmd, function (err) {
        if (err) { return k(err); }

        debug('annotating', names);
        client.get(cmd, {screen_name: names.join(',')},
            function (err, annotations) {
                if (err) { return k(err); }
                annotations.forEach(function (nfo) {
                    saveLookup(nfo);
                });
                return k();
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
                if (err) { return k(err); }
                debug('fetched', id, ids.ids.slice(0,10) + '...');
                saveFollowers(id, ids.ids);
                k();
            });
    });
}


//====================

function addAnnotations () {
    setInterval(function () {
        debug('try updating names');
        if (limits['/users/lookup'].remaining) {
            var incomplete =
                _.keys(userIds)
                    .filter(function (id) { return !idToAccount[id] || !idToAccount[id].nfo; })
                    .slice(0, 100);
            if (incomplete.length > 50) {
                annotateIds(incomplete, function (err) {
                    if (err) { return console.error('failed to annotate names', err); }
                    debug('annotated extra names');
                });
            } else {
                debug('not enough extras to annotate');
            }
        }
    }, 1000);
}
//====================


function explore (seeds, amt, k) {

    var id;
    if (seeds.length) {
        id = seeds.pop();
    }
    if (id === undefined) {
        var max = 1000;
        var ids = _.keys(userIds);
        while (id === undefined) {
            if (!max--) { return k('exhausted'); }
            var idx = Math.round(Math.random() * (ids.length - 1));
            id = ids[idx];
            if (idToAccount[id] && idToAccount[id].followers) {
                //keep fishing
                id = undefined;
            }
        }
    }

    var proceed = function (err) {
        if (err) { return k(err); }
        debug('expanding', id, idToAccount[id].nfo.screen_name);
        followers(
            id,
            function (err) {
                if (err) { return k(err); }
                debug('expanded', id, idToAccount[id].followers.length);
                explore(seeds, amt - 1, k);
        });
    };

    if (!idToAccount[id] || !idToAccount[id].nfo) {
        debug('expanding but first annotating', id);
        annotateIds([id], proceed);
    } else {
        proceed();
    }
}

function crawler (seeds, amt, k) {

    debug('crawling with seeds', seeds);

    var missingSeeds = seeds.filter(function (name) { return !accounts[name]; });
    if (missingSeeds.length) { debug('first annotate', missingSeeds); }
    annotateNames(missingSeeds, function (err) {
        if (err) { return k(err); }
        if (missingSeeds.length) { debug('annotated missing'); }

        debug('start exploring');
        explore(
            seeds
                .filter(function (name) { return accounts[name] && !accounts[name].followers; })
                .map(function (name) { return accounts[name].id; }),
            amt - 1,
            k);
    });
}


updateLimits(function (err, limits) {
    if (err) { return console.error('bad limits', err); }

    addAnnotations();

    crawler(SEEDS, MAX_RET, function (err, network) {
        if (err) { return console.error('error', err); }
        else {
            debug(
                'done',
                _.keys(accounts)
                    .filter(function (name) { return accounts[name].followers; }));
            }
    });
});


