var r;
var credentials;

var logging = true; // SET TO FALSE
var log_errors = true;

module.exports = {
	logging: function(to_log) {
		logging = to_log;
	},

	error_logging: function(err_log) {
		log_errors = err_log;
	},

	set_credentials: function(creds, try_connect) {
		credentials = creds;

		if (try_connect)
			r = conn();
	},

	reset_credentials: function() {
		r = undefined;
	},

	connect: function() {
		r = conn();
	},

	create_table: function(table, callback) {
		if (!r) return;

		r.tableCreate(table)
		.run()
		.then(function(response){
			log('Create_Table Success: ' + response);

			if (callback)
				callback(response)
		})
		.error(function(err){
			log_error('Error while creating table: ' + err)
			return;
		})

		return;
	},

	insert: function(table, to_insert, callback) {
		if (!validate_conn())
			return;

		r.table(table)
		.insert(to_insert)
		.run()
		.then(function(response){
			log('Insert Successful: ' + response);
			if (callback) callback(response);
		})
		.error(function(err){
			log_error(err);
		})
	},

	delete: function (table, to_delete, callback) {
		r.table(table)
		.get(to_delete)
		.delete()
		.then(function(response){
			if (callback) callback(response);
		})
		.error(function(err){
			log_error(err)
		})
	},

	get_all: function(table, callback) {
		if (!validate_conn())
			return;

		r.table(table)
		.run()
		.then(function(response){
			if (callback) callback(response);
		})
		.error(function(err){
			log_error(err);
		})
	},

	subscribe: function (table, callback) {
		if (!validate_conn())
			return;

		r.table(table)
		.changes()
		.run()
		.then(function(cursor){
			cursor.each(callback)
		})
		.error(function(err){
			console.log(err);
		});
	}

}

function conn() {
	if (!credentials) {
		log_error("Credientials have not been set up. Please call 'set_credentials' first.")
		return undefined;
	}

	var r = require('rethinkdbdash')(credentials);
	return r;
}

function validate_conn() {
	if (!r) {
		log_error("No connection is established. Try running connect()");
		return false;
	}

	return true;
}

function log_error(err) {
	if (log_errors)
		console.log("Error in Rethink-Helper: ", err);
}

function log(details) {
	if (logging)
		console.log("Log from RH: ", details);
}