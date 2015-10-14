/* 
* @Author: bhargavkrishna
* @Date:   2015-10-14 00:53:35
* @Last Modified by:   bhargavkrishna
* @Last Modified time: 2015-10-15 00:46:28
*/

'use strict';

/**
 * A client that makes requests to Elasticsearch
 *
 * Initializing a client might look something like:
 *
 * ```
 * var client = new es.Client({
 *     host: '127.0.0.1',
 *     port: '9001',
 *     index: 'xxx',
 *     type: 'BB'
 * });
 * ```
 *
 * @class Client
 */

var exports = require('./esExport.js'),
	imports = require('./esImport.js');

function Client(config) {
    //initializing a new object if  not invoked with 'new'
    if(!(this instanceof Client))
        return new Client(config);
    var params = ['host', 'port', 'index', 'type'];
    for (var i = 0; i < params.length; i++) {
        if(!config[params[i]])
            throw params[i] + ' not found.';
    }
    this.exports = new exports(config);
    this.imports = new imports(config);
}

module.exports = Client;