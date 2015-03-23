# followerfollower

Given some seed account names, randomly expand a twitter network.

To use:

1. npm install
2. go to twitter and create an app w/ auth tokens
3. cp config.json.template config.json
4. modify config.json with auth info
5. run 'node main.js' to start filling in 'accounts.json'
6. run graphistry etl server and then 'node accountsToVgraph.js' to upload dataset twitter2
