/* 
* @Author: bhargavkrishna
* @Date:   2015-10-14 00:33:20
* @Last Modified by:   bhargavkrishna
* @Last Modified time: 2015-10-14 03:04:53
*/

/**
 * import utilities on Elasticsearch
 *
 * @class Import
 */

'use strict';
var Q = require('q'),
    request = require('request'),
    stream = require('stream');

var esConfig,
    toBeInserted = [],
    BULK_SIZE = 100000,
    bulkNum = 0;

function Import(config) {
    esConfig = config;
}

function addRecordToEs(data) {
    toBeInserted.push(data);
    if(toBeInserted.length === BULK_SIZE) {
        console.log('process bulk : ' + bulkNum);
        addRecordsToEsBulk(toBeInserted);
        toBeInserted = [];
        bulkNum++;
    }
}

function addRecordsToEsBulk(records) {
    var payload,
        esUrl,
        start = new Date();
    records = records.map(function(record) {
        if(typeof record !== 'string')
            return JSON.stringify(record);
        return record;
    });
    payload = records.join('\n{ "index" : { "_index" :esConfig.index, "_type" : esConfig.type} }\n');
    payload += '\n';
    payload = '{ "index" : { "_index" :esConfig.index, "_type" : esConfig.type} }\n' + payload;
    var options = {
        hostname: esConfig.host,
        port: esConfig.port,
        path: '/' + esConfig.index + '/' + esConfig.type + '/_bulk',
        method: 'POST',
        headers: {
            'Content-Type': 'text/plain;charset=UTF-8',
            'Content-Length': Buffer.byteLength(payload)
        }
    };

    var req = http.request(options, function(res) {
      console.log('status code of insert num :' + bulkNum + ' is ' + res.statusCode);
      res.setEncoding('utf8');
      var error = [];
      res.on('data', function (chunk) {
        if(res.statusCode !== 200)
            error.push(chunk);
        else {
            req.abort();
            if(bulk_num)
                console.log('current bulk count :' + bulkNum + ' done');
            var timeTaken = new Date() - start;
            console.log("records added to ES took :" + timeTaken);
        }
      });
      res.on('end', function() {
        if(res.statusCode !== 200) {
            console.log('Error :' + error.join(''));
        }
        var timeTaken = new Date() - start;
        console.log("records added to ES took (req end) :" + timeTaken);
      })
    });

    req.on('error', function(e) {
        console.log(e);
        console.log('problem with request: ' + e.message);
        console.log('Failed records :' + records.join(','));
    });

    // write data to request body
    req.write(payload);
    req.end();
}

/**
 * Inserts records to elastic search using streams and bulk api
 * 
 * @param  {Array | stream} list or stream of records to be inserted
 * @param {integer} bulkSize increase or decrease bulksize based on network latency defaults to 100000
 */
function insertRecords(data, bulkSize) {
    BULK_SIZE = bulkSize || BULK_SIZE;
    if(data instanceof stream) {
        stream.on('data', function(data) {
            addRecordToEs(data);
        });
        stream.on('end', function() {
            if(toBeInserted.length !== 0) {
                addRecordsToEsBulk(toBeInserted);
                toBeInserted = [];
                bulkNum++;
            }
            console.log('All trips processed.');
        });
    }
    else if(data instanceof Array){
        for(var i=0; i<data.length; i++ ) {
            addRecordToEs(data);
        }
        if(toBeInserted.length !== 0) {
            addRecordsToEsBulk(toBeInserted);
            toBeInserted = [];
            bulkNum++;
        }
    }
    else 
        throw "Unsupported Type "
}

Import.prototype.insertRecords = insertRecords;

module.exports = Import;