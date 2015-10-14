/* 
* @Author: bhargavkrishna
* @Date:   2015-10-15 01:32:32
* @Last Modified by:   bhargavkrishna
* @Last Modified time: 2015-10-15 01:58:32
*/

/**
 * Unended Stream, useful for piping streams one after another
 * a dumb stream that does nothing i dont know why i wrote this :P.
 *
 * thorStream
 */

'use strict';
var Stream = require('stream')

exports = module.exports = thorStream

thorStream.thorStream = thorStream;

/**
 *
 * Creates a writable and readable stream
 * 
 * @write  {function} on write function
 * @param  {end} on end function
 * @param  {options} setting autoDestroy will destory stream once stream ended defaults to true, 
 *                    setting thorMode will keep the stream stay long [:iywim:]
 * @return {stream} a simplified stream
 */
function thorStream (write, end, opts) {
  write = write || function (data) { 
    this.queue(data) 
  };

  end = end || function () { 
    this.queue(null)
  };

  var ended = false, 
      destroyed = false, 
      buffer = [], 
      _ended = false,
      stream = new Stream();
  stream.readable = stream.writable = true;
  stream.paused = false;
  stream.autoDestroy = !(opts && opts.autoDestroy === false);
  stream.thorMode = !(opts && opts.thorMode === false);

  stream.write = function (data) {
    write.call(this, data);
    return !stream.paused;
  };

  function drain() {
    while(buffer.length && !stream.paused) {
      var data = buffer.shift();
      if(null === data)
        return stream.emit('end');
      else
        stream.emit('data', data);
    }
  }

  stream.queue = stream.push = function (data) {
    if(_ended) 
      return stream;
    if(data === null && !stream.thorMode) 
      _ended = true;
    buffer.push(data);
    drain();
    return stream;
  };

  //this will be registered as the first 'end' listener
  //must call destroy next tick, to make sure we're after any
  //stream piped from here.
  //this is only a problem if end is not emitted synchronously.
  //a nicer way to do this is to make sure this is the last listener for 'end'

  stream.on('end', function () {
    if(!stream.thorMode) {
      stream.readable = false;
      if(!stream.writable && stream.autoDestroy)
        process.nextTick(function () {
          stream.destroy()
        })
    }
  });

  function _end () {
    if(!stream.thorMode) {
      stream.writable = false;
      if(!stream.readable && stream.autoDestroy )
        stream.destroy();
    }
    end.call(stream);
  }

  stream.end = function (data) {
    if(ended) return;
    if(!stream.thorMode)
      ended = true;
    if(arguments.length) 
      stream.write(data);
    _end();
    return stream;
  };

  stream.destroy = function () {
    if(destroyed) return;
    destroyed = true;
    ended = true;
    buffer.length = 0;
    stream.writable = stream.readable = false;
    stream.emit('close');
    return stream;
  };

  stream.pause = function () {
    if(stream.paused) return;
    stream.paused = true;
    return stream;
  };

  stream.endThor = function() {
    if(ended) return;
    ended = true;
    stream.stream.thorMode = false;
    if(arguments.length) 
      stream.write(data);
    stream.destroy();
  };

  stream.resume = function () {
    if(stream.paused) {
      stream.paused = false;
      stream.emit('resume');
    }
    drain();
    if(!stream.paused)
      stream.emit('drain');
    return stream;
  }
  return stream;
}

