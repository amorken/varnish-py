#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Copyright (c) 2012 Giacomo Bagnoli <g.bagnoli@asidev.com>

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
from datetime import datetime
import inspect
import logging
import functools
from .api import logs
from .utils import MultiDict
log = logging.getLogger(__name__)


class VarnishLogs(object):
    default_settings = {
           'process_old_entries': False,
           'process_backend_requests': False,
           'process_client_requests': False,
           'include_tag': None,
           'include_tag_regex': None,
           'exclude_tag': None,
           'exclude_tag_regex': None,
           'stop_after': None,
           'skip_first': None,
           'read_entries_from_file': None,
           'filter_transactions_by_tag_regex': None,
           'ignore_case_in_regex': False
        }

    def __init__(self, varnish, **settings):
        self.settings = dict(self.default_settings)
        self.settings.update(settings)
        self.varnish = varnish
        self.vd = varnish.vd
        logs.init(self.vd, True)
        for st, value in self.settings.items():
            if value and self.default_settings[st] is None:
                getattr(logs, st)(self.vd, value)

            elif value and self.default_settings[st] is False:
                getattr(logs, st)(self.vd)

            if value and st == "filter_transactions_by_tag_regex":
                getattr(logs, st)(self.vd, value[0], value[1])

    def __getattr__(self, attr):
        if attr in self.default_settings:
            return functools.partial(getattr(logs, attr), self.vd)

        raise AttributeError(attr)

    def dispatch_chunks(self, callback, source=None):
        """ Read logs from varnish shared memory logs, then call callback
            for every chunk as returned from the low level api
            `callback` must be a callable that accepts 0 or 1 positional
            parameter (an instance of the varnish.api.logs.LogChunk class)
        """
        if callback:
            args = len(inspect.getargspec(callback).args)

        def wrapper(chunk, priv):
            res = False

            if callback and args == 0:
                res = callback()

            elif callback:
                res = callback(chunk)

            return res

        if source:
            self.read_entries_from_file(source)

        logs.dispatch(self.vd, wrapper)

    def dispatch_requests(self, callback, aggregate=True, source=None,
                          nonrequest_callback=None):
        """ Read logs from Varnish shared memory Logs, then call callback
            when a RequestLog is complete (all its chunks have been read).
            `callback` must be a callable that accepts 1 positional parameter
            (an instance of ClientRequestLog or BackendRequestLog, subclasses
             of RequestLog)
            if `aggregate` is true, try to relate backend requests to the
            client request that generated it
            `nonrequest_callback` (optional), if set, must be a callable that
            accepts 1 positional parameter (an instance of
            varnish.api.logs.LogChunk) which will be invoked for log lines not
            related to any individual request.
        """
        # These log tags are not associated with any particular request.
        from varnish.api.logs import name_to_tag
        _ignored_tags = set([name_to_tag(tag) for tag in
                                ["sessionopen", "sessionclose", "statsess"]])
        def cb(chunk):
            res = False
            try:
                if chunk.fd == 0:
                    if nonrequest_callback:
                        return nonrequest_callback(chunk)
                    else:
                        return True

                # Discard log chunks associated with a client session
                # but not a particular request.
                if chunk.tag in _ignored_tags:
                    return True

                ev = RequestLog(chunk)
                # discard invalid, incomplete and empty requests
                res = True
                if not ev or not ev.complete or (ev.backend and not ev.chunks):
                    return res
                
                # If aggregate, only return client requests with references to backend requests,
                # otherwise return client and backend requests alike.
                if ev.complete and ((aggregate and ev.client) or (not aggregate)):
                    res = callback(ev)

            except:
                from traceback import print_exc
                print_exc()
                pass # Keep on going.
            return res

        self.dispatch_chunks(callback=cb, source=source)

    def __str__(self):
        return "<%s [instance: %s]>" % (self.__class__.__name__,
                                        self.varnish.name)

    def __repr__(self):
        return str(self)


class RequestLog(object):
    """ This class is a factory for its subclasses. It keeps returning the
        same objects as long as the chunk belongs to an existing instance.
        It returns None if the chunk belongs to neither a client of backend
        request
    """
    _lines = {}

    def __new__(cls, chunk, active=False):
        if chunk.fd in cls._lines:
            obj = cls._lines[chunk.fd]
            if chunk.client:
                assert isinstance(obj, ClientRequestLog), \
                        "Found %s when processing client request log chunk %s" % \
                            (repr(obj), repr(chunk))
            elif chunk.backend:
                assert isinstance(obj, BackendRequestLog), \
                        "Found %s when processing backend request log chunk %s" % \
                            (repr(obj), repr(chunk))

        else:
            if chunk.client:
                obj = super(RequestLog, cls).__new__(ClientRequestLog)

            elif chunk.backend:
                obj = super(RequestLog, cls).__new__(BackendRequestLog)

            else:
                return None

            obj.init(chunk.fd, active)
            cls._lines[chunk.fd] = obj

        obj.add_chunk(chunk)
        return obj

    def init(self, fd, active=False):
        if hasattr(self, "fd"):
            return

        self.fd = fd
        self.chunks = []
        self.active = active
        self.complete = False
        self.rxheaders = MultiDict()
        self.txheaders = MultiDict()
        self.rxprotocol = None
        self.txprotocol = None
        self.method = None
        self.url = None
        self.status = None
        self.response = None
        self.length = None

    def add_chunk(self, chunk):
        if self.fd == 0:
            return False

        if not self.active and \
            ((chunk.client and chunk.tag.name == 'reqstart') or
             (chunk.backend and chunk.tag.name == 'backendopen')):
            self.active = True

        elif not self.active:
            return False

        if (chunk.client and chunk.tag.name == 'reqend') or \
           (chunk.backend and (chunk.tag.name == 'backendclose' or
                               chunk.tag.name == 'backendreuse')):
            self.complete = True
            self.active = False
            if chunk.client:
                del self.__class__._lines[self.fd]

        self.on_append_chunk(chunk)
        return self.complete

    def on_append_chunk(self, chunk):
        self.client = chunk.client
        self.backend = chunk.backend
        self.chunks.append(chunk)
        name = chunk.tag.name
        if name == 'rxheader':
            key, value = chunk.data.split(":", 1)
            self.rxheaders[key.strip().lower()] = value.strip()

        elif name == 'txheader':
            key, value = chunk.data.split(":", 1)
            self.txheaders[key.strip().lower()] = value.strip()

        elif name == 'rxprotocol':
            self.rxprotocol = chunk.data

        elif name == 'txprotocol':
            self.txprotocol = chunk.data

        elif name == 'length':
            self.length = int(chunk.data)

    def __repr__(self):
        res = "<%s %s" % (self.__class__.__name__, self.id)
        for c in self.chunks:
            res += "\n\t%s" % (c)
        res += ">"
        return res


class ClientRequestLog(RequestLog):
    """ Aggragates chunks for a client request """

    def init(self, fd, active=False):
        super(ClientRequestLog, self).init(fd, active)
        self.id = None
        self.vcl_calls = MultiDict()
        self.hash_data = []
        self.client_ip = None
        self.client_port = None
        self.started_at = None
        self.completed_at = None
        self.req_start_delay = None
        self.processing_time = None
        self.deliver_time = None
        self.backend_request = None

    def on_append_chunk(self, chunk):
        super(ClientRequestLog, self).on_append_chunk(chunk)
        name = chunk.tag.name

        if name == 'vcl_call':
            self._last_vcl = chunk.data

        elif name == 'vcl_return':
            self.vcl_calls[self._last_vcl] = chunk.data
            del self._last_vcl

        elif name == 'hash':
            self.hash_data.append(chunk.data)

        elif name == 'length':
            self.length = int(chunk.data)

        elif name == 'rxrequest':
            self.method = chunk.data

        elif name == 'rxurl':
            self.url = chunk.data

        elif name == 'reqstart':
            self.client_ip, self.client_port, self.id = \
                chunk.data.split(" ")

        elif name == 'txstatus':
            self.status = int(chunk.data)

        elif name == 'txresponse':
            self.response = chunk.data

        elif name == 'backend':
            backend_fd, director, backend = chunk.data.split(" ")
            backend_fd = int(backend_fd)
            
            known_fds = super(ClientRequestLog, self.__class__)._lines
            
            if backend_fd in known_fds:
                self.backend_request = known_fds[backend_fd]
                assert isinstance(self.backend_request, BackendRequestLog), \
                        "Processing chunk %s: fd %s in request table should have been a BackendRequestLog object, was %s" % (repr(chunk), backend_fd, repr(self.backend_request))
            else:
                self.backend_request = super(RequestLog, self.__class__)\
                                            .__new__(BackendRequestLog)
                self.backend_request.init(backend_fd)
                self.backend_request.backend_name = backend
                known_fds[backend_fd] = self.backend_request

            # Directors are not named in the backend log lines themselves, so we propagate it from here.
            if not self.backend_request.director_name:
                self.backend_request.director_name = director

        elif name == 'reqend':
            xid, started_at, completed_at, \
                req_start_delay, processing_time, \
                deliver_time = chunk.data.split(" ")
            self.started_at = datetime.fromtimestamp(float(started_at))
            self.completed_at = datetime.fromtimestamp(float(completed_at))
            self.req_start_delay = float(req_start_delay)
            self.processing_time = float(processing_time)
            self.deliver_time = float(deliver_time)

    @property
    def hit(self):
        return "hit" in self.vcl_calls

    @property
    def miss(self):
        return "miss" in self.vcl_calls

    def __repr__(self):
        indented_bereq="    ".join(repr(self.backend_request).splitlines(True))
        res = """
<{self.__class__.__name__} XID: {self.id}
    Client: {self.client_ip}:{self.client_port}

    Timing:
        started   : {self.started_at}
        completed : {self.completed_at}
        delay     : {self.req_start_delay} [s]
        processing: {self.processing_time} [s]
        deliver   : {self.deliver_time} [s]

    Request: {self.rxprotocol} {self.method} {self.url}
        headers   : {self.rxheaders}

    Backend Request: {bereq}

    Hash: {self.hash_data}
    VCL Calls: {self.vcl_calls}

    Response: {self.txprotocol} {self.status} {self.response} [{self.length}B]
        headers   : {self.txheaders}
>""".format(self=self, bereq=indented_bereq)
        return res

    def __str__(self):
        return "<{self.__class__.__name__} XID: {self.id}>".format(self=self)


class BackendRequestLog(RequestLog):
    """ Aggregates chunks for a backend request """

    def init(self, fd, active=False):
        super(BackendRequestLog, self).init(fd, active)
        self.backend_name = None
        self.director_name = None

    def on_append_chunk(self, chunk):
        log.debug("BackendRequestLog: appending %s to %s" % (chunk, repr(self)))
        super(BackendRequestLog, self).on_append_chunk(chunk)
        name = chunk.tag.name
        if name == 'txrequest':
            self.method = chunk.data

        elif name == 'txurl':
            self.url = chunk.data

        elif name == 'rxstatus':
            self.status = int(chunk.data)

        elif name == 'rxresponse':
            self.response = chunk.data

        elif name in ('backendopen', 'backendreuse'):
            self.backend_name = chunk.data.split(" ")[0]

    def __repr__(self):
        return """
<{self.__class__.__name__} [backend: {self.backend_name}]
    Request: {self.rxprotocol} {self.method} {self.url}
        headers   : {self.rxheaders}

    Response: {self.txprotocol} {self.status} {self.response} [{self.length}B]
        headers   : {self.txheaders}
>""".format(self=self)

    def __str__(self):
        return "<{self.__class__.__name__} ({self.backend_name})>"\
                .format(self=self)
