/*
* @Author: bhargavkrishna
* @Date:   2015-10-13 18:53:37
* @Last Modified by:   bhargavkrishna
* @Last Modified time: 2015-10-15 01:56:56
*/

/**
 * export utilities on Elasticsearch
 *
 * @class Export
 */


'use strict';
var Q = require('q'),
    request = require('request'),
    JSONStream = require('JSONStream'),
    thorStream = require('./thorStream.js');

var esConfig,
    esBulkCount = 0;

function getEsUrl(config) {
    var esUrl = 'http://' + config.host + ':' + config.port +'/' + config.index + '/' + config.type + '/';
    return esUrl;
}

function getEsScrollId(size, scrollId) {
    var deferred = Q.defer(),
        esUrl;
    console.log('getting ES scrollId');
    if(scrollId)
        deferred.resolve(body._scroll_id);
    else {
        esUrl = getEsUrl(esConfig) + '_search?scroll=1w&search_type=scan&size=' + size;
        request(esUrl, function (error, response, body) {
            if (!error && response.statusCode == 200) {
                body = JSON.parse(body);
                console.log('ES scroll_id :' + body._scroll_id);
                deferred.resolve(body._scroll_id);
            }
            else {
                deferred.reject(error);
            }
        });
    }
    return deferred.promise;
}

function getDataFromScrollId(scrollId, stream) {
    console.log('getting data from ES for bulk num :' + esBulkCount);
    esBulkCount++;
    var esUrl;
    esUrl = 'http://' + esConfig.host + ':' + 
            esConfig.port  + 
            '/_search/scroll?scroll=1w&search_type=scan&scroll_id=' + scrollId;
    return request({url: esUrl})
        .pipe(JSONStream.parse('hits.hits.*'))
        .pipe(stream);
}

/**
 * Get all data from elastic search using scroll and scan
 * 
 * @param  {int} bulkSize - Scroll speed increase or decrease based on network latency.
 * @param  {string} scrollId - Uses this scrollId to continue on prev. scroll.
 * @return {stream} Stream of records one at a time.
 */
function getAllData(bulkSize, scrollId) {
    bulkSize = bulkSize || 100000;

    var stream,
        count = 0;

    stream = thorStream(function write(data) {
        //console.log(data);
        count++;
        this.emit('data', data);
    },
    function end () {
        if(count === 0) {
            this.endThor();
            this.emit('end');
            return;
        }
        count = 0;
        getDataFromScrollId(scrollId, stream);
    });
    stream.autoDestroy = false;
    getEsScrollId(bulkSize, scrollId).then(function(data) {
        scrollId = data;
        getDataFromScrollId(scrollId, stream);

    }, function(err) {
        throw err;
    });
    return stream;
}

function getPaginatedRes(query, from, size, stream) {
    var esUrl,
        options;
    esUrl = getEsUrl() + '_search?from=' + from + '&size=' + size;
    options = {
        url: esUrl,
        method: 'POST',
        body: query
    };
    return request(options)
        .pipe(JSONStream.parse('hits.hits.*'))
        .pipe(stream);
}

/**
 * Get Data matching the query 
 * warning: if the data is being inserted while querying you might see some duplicates because of sorting
 * hack: you can try the query to sort on insertion date in ascending order.
 * 
 * @param  {string} query - Query as a json string
 * @param  {string} size - Increase or decrease size based on network latency defaults to 100000
 * @return {stream} Stream of records one at a time.
 */
function getDataByQuery (query, size) {
    size = size || 100000;
    if(!query)
        throw "Use getAllData for empty queries";
    if(typeof query !== 'string')
        query = JSON.stringify(query);

    var stream,
        count = 0,
        from = 0;

    stream = through(function write(data) {
        count++;
        this.emit('data', data);
    },
    function end () {
        if(count === 0) {
            this.emit('end');
            return;
        }
        count = 0;
        from = (from + 1)*size;
        getPaginatedRes(query, from, size, stream);
    });
    return stream;
}

function Export(config) {
    console.log('initializing export object');
    esConfig = config;
}

Export.prototype.getAllData = getAllData;
Export.prototype.getDataByQuery = getDataByQuery;

module.exports = Export;