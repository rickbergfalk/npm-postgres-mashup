var yetAnotherNpm = require('./npm-postgres-mashup.js');

yetAnotherNpm.copyTheData({
	couchHost: 'localhost',
	couchDatabase: 'mynpm',
	postgresHost: 'localhost',
	postgresDatabase: 'npm',
	postgresUser: 'npmadmin',
	postgresPassword: 'testadmin',
	beNoisy: true,
	iUnderstandThisDropsTables: true, 
	theFinalCallback: function (err) {
		if (err) throw err;
		console.log('all done from the final callback');
		process.exit(0)
	}
});