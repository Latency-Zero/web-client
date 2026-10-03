/**
 * LatZero Web Client
 * AUTHOR: BRAHMAI (https://brahmai.in)
 * 
 * A comprehensive browser-compatible client that combines:
 * - Direct WebSocket connection and buffer operations from latzero-web-direct.js
 * - Process pool functionality from latzero-web.js
 */

(function(global) {
    'use strict';

    class LatZeroWebClient extends EventTarget {
        constructor(dsn, pool, options = {}) {
            super();
            
            // Parse DSN: latzero://client-id
            const parsed = this.parseDSN(dsn);
            if (!parsed) {
                throw new Error('DSN must look like latzero://client-id');
            }
            
            this.clientId = parsed.clientId;
            this.poolName = pool;
            this.authToken = options.authToken || null;
            this.host = options.host || '127.0.0.1';
            this.port = options.port ?? 14130;
            this.wsPort = options.wsPort ?? this.port + 1;
            this.wsProtocol = options.wsProtocol ?? 'ws';
            this.maxRedirects = options.maxRedirects ?? 4;
            this.timeout = options.timeout ?? 5000;
            this.maxPendingRequests = options.maxPendingRequests ?? 256;
            this.maxBatchSize = options.maxBatchSize ?? 128;
            this.maxPendingBatches = options.maxPendingBatches ?? 8;
            this.maxActiveHandlers = options.maxActiveHandlers ?? 32;
            this.maxQueuedMessages = options.maxQueuedMessages ?? 256;
            this.maxQueuedBytes = options.maxQueuedBytes ?? 4 * 1024 * 1024;
            this.maxBufferedAmount = options.maxBufferedAmount ?? 1024 * 1024;
            this.maxFrameBytes = options.maxFrameBytes ?? 1024 * 1024;
            this.sendPollInterval = options.sendPollInterval ?? 10;
            
            // Connection state
            this.ws = null;
            this.connected = false;
            this.pending = new Map(); // request_id -> typed waiter with one deadline
            this.eventHandlers = new Map(); // event -> [handlers]
            this.reconnectAttempts = 0;
            this.maxReconnectAttempts = options.maxReconnectAttempts ?? 5;
            this.reconnectDelay = options.reconnectDelay ?? 1000;
            this.maxReconnectDelay = options.maxReconnectDelay ?? 30000;
            this._generation = 0;
            this._requestSequence = 0;
            this._connecting = null;
            this._endpoint = null;
            this._entryEndpoint = null;
            this._reconnectTimer = null;
            this._intentionalDisconnect = false;
            this._outbox = [];
            this._queuedBytes = 0;
            this._sendTimer = null;
            this._activeHandlers = new Set();
            this._handlerWaiters = new Set();
            this._activeBatches = 0;
            this._switching = false;
            this._switchOperation = null;
            this._deferredNotifications = [];
            this._deferredNotificationBytes = 0;

            for (const key of ['timeout', 'reconnectDelay', 'maxReconnectDelay', 'sendPollInterval']) {
                if (!Number.isFinite(this[key]) || this[key] <= 0 || this[key] > 2147483647) {
                    throw new RangeError(`${key} must be a positive finite millisecond duration <= 2147483647`);
                }
            }
            for (const key of ['maxPendingRequests', 'maxBatchSize', 'maxPendingBatches',
                'maxActiveHandlers', 'maxQueuedMessages', 'maxQueuedBytes',
                'maxBufferedAmount', 'maxFrameBytes']) {
                if (!Number.isSafeInteger(this[key]) || this[key] <= 0) {
                    throw new RangeError(`${key} must be a positive safe integer`);
                }
            }
            if (!Number.isSafeInteger(this.maxReconnectAttempts) || this.maxReconnectAttempts < 0) {
                throw new RangeError('maxReconnectAttempts must be a nonnegative safe integer');
            }
            if (!Number.isSafeInteger(this.maxRedirects) || this.maxRedirects < 0 || this.maxRedirects > 16) {
                throw new RangeError('maxRedirects must be a safe integer between 0 and 16');
            }
            this._configuredEndpoint();
            
            // Process pool state
            this._processes = new Map(); // short process name -> handler and registration context
            
            // Browser handlers run on the main thread, not in WebWorkers.
            const _self = this;
            this.process = {
                /**
                 * Register a function as a named process.
                 * Name is inferred from fn.name, or pass an explicit name as second arg.
                 *
                 *   await client.process.register(myFn);
                 *   await client.process.register(myFn, 'override-name');
                 *   await client.process.register(x => x, 'square');  // anon: explicit required
                 */
                register: async (fn, nameOverride = null, options = {}) => {
                    if (typeof fn !== 'function') throw new TypeError('Process handler must be a function');
                    const name = nameOverride || fn.name;
                    if (!name) {
                        throw new Error(
                            'Anonymous functions must have an explicit name: register(fn, "name")'
                        );
                    }

                    const previous = _self._processes.get(name);
                    if (previous?.updating) throw _self._error('registration_in_progress', 'Process registration is in progress');
                    const record = { fn, context: _self._context(), updating: true };
                    _self._processes.set(name, record);
                    try {
                        await _self.sendRequest('register_process', {
                            process_name: name,
                            worker_kind: options.workerKind || 'thread',
                            min_workers: options.minWorkers ?? 1,
                            max_workers: options.maxWorkers ?? 10,
                        });
                        record.updating = false;
                    } catch (error) {
                        if (_self._processes.get(name) === record) {
                            if (previous) _self._processes.set(name, previous);
                            else _self._processes.delete(name);
                        }
                        throw error;
                    }
                },

                /**
                 * Unregister a process by its short name.
                 */
                unregister: async (name) => {
                    const previous = _self._processes.get(name);
                    if (previous?.updating) throw _self._error('registration_in_progress', 'Process registration is in progress');
                    const record = { fn: null, context: _self._context(), updating: true };
                    _self._processes.set(name, record);
                    try {
                        await _self.sendRequest('unregister_process', { process_name: name });
                        if (_self._processes.get(name) === record) _self._processes.delete(name);
                    } catch (error) {
                        if (_self._processes.get(name) === record) {
                            if (previous) _self._processes.set(name, previous);
                            else _self._processes.delete(name);
                        }
                        throw error;
                    }
                },

                /**
                 * Call a process by its full process_id.
                 * process_id format: "client_id:process_name"
                 */
                call: async (processId, data = {}, options = {}) => {
                    const timeoutMs = options.timeout ?? _self.timeout;
                    return _self.sendRequest('call_process', {
                        process_id: processId,
                        data,
                        response_to: options.responseTo || null,
                        timeout: timeoutMs / 1000
                    }, undefined, { timeout: timeoutMs });
                },

                /**
                 * Broadcast to all processes registered under the given short name.
                 * Returns the list of process_ids that were invoked.
                 */
                broadcast: async (processName, data = {}, options = {}) => {
                    const timeoutMs = options.timeout ?? _self.timeout;
                    const response = await _self.sendRequest('broadcast_process', {
                        process_name: processName,
                        data,
                        response_to: options.responseTo || null,
                        timeout: timeoutMs / 1000
                    }, undefined, { timeout: timeoutMs });
                    return response.payload?.invoked_processes || [];
                },

                /**
                 * List all registered processes in the pool.
                 * pattern optionally filters by client_id prefix.
                 */
                list: async (pattern = null) => {
                    const response = await _self.sendRequest('list_processes', { pattern });
                    return response.payload?.processes || {};
                }
            };
            
            // Auto-connect if not disabled
            if (options.autoConnect !== false) {
                this.connect().catch(() => {}); // Failures are also published on the error event.
            }
        }
        
        parseDSN(dsn) {
            try {
                const url = new URL(dsn.replace('latzero://', 'http://'));
                return {
                    clientId: url.hostname
                };
            } catch (e) {
                return null;
            }
        }
        
        connect() {
            if (this._connecting) return this._connecting.promise;
            if (this.connected && this.ws?.readyState === WebSocket.OPEN) return Promise.resolve();
            if (this.ws) this._closeConnection(this._error('connection_lost', 'Previous socket is not open'));
            this._intentionalDisconnect = false;
            clearTimeout(this._reconnectTimer);
            this._reconnectTimer = null;

            const attempt = this._connectionAttempt(this._deadline(), this.poolName, this.authToken);
            try {
                const endpoint = this._configuredEndpoint();
                attempt.redirects = this._redirectFlow(endpoint);
                this._openConnection(attempt, endpoint);
            } catch (error) {
                this._closeConnection(error);
            }
            return attempt.promise;
        }

        get endpoint() {
            return this._endpoint;
        }

        _configuredEndpoint() {
            return this._makeEndpoint(this.host, this.port, this.wsPort, this.wsProtocol);
        }

        _makeEndpoint(host, port, wsPort, protocol) {
            for (const [key, value] of [['port', port], ['wsPort', wsPort]]) {
                if (!Number.isInteger(value) || value < 1 || value > 65535) {
                    throw new RangeError(`${key} must be an integer between 1 and 65535`);
                }
            }
            if (protocol !== 'ws' && protocol !== 'wss') throw new TypeError('wsProtocol must be ws or wss');
            if (typeof host !== 'string' || !host || host.trim() !== host || /[/\\?#@%]/.test(host)) {
                throw new TypeError('host must be a hostname or numeric IP address, not a URL');
            }
            const authority = host.includes(':') && !host.startsWith('[') ? `[${host}]` : host;
            const url = new URL(`${protocol}://${authority}:${wsPort}/`);
            if (url.username || url.password || url.pathname !== '/' || url.search || url.hash) {
                throw new TypeError('host must not contain URL credentials, paths or query strings');
            }
            return Object.freeze({ host, port, wsPort, protocol, url: url.href });
        }

        _loopbackHost(host, allowLocalhost = false) {
            if (typeof host !== 'string') return null;
            if (allowLocalhost && host.toLowerCase() === 'localhost') return 'localhost';
            const parts = host.split('.');
            if (parts.length === 4 && parts[0] === '127' &&
                parts.every(part => /^(0|[1-9]\d{0,2})$/.test(part) && Number(part) <= 255)) return host;
            const address = host.startsWith('[') && host.endsWith(']') ? host.slice(1, -1) : host;
            if (!address.includes(':') || !/^[0-9a-f:]+$/i.test(address)) return null;
            try {
                return new URL(`http://[${address}]/`).hostname === '[::1]' ? '::1' : null;
            } catch (_) { return null; }
        }

        _redirectFlow(endpoint, initial = this._configuredEndpoint()) {
            const visited = new Set([endpoint.url, initial.url]);
            if (this._loopbackHost(initial.host, true) === 'localhost') {
                for (const host of ['127.0.0.1', '::1']) {
                    visited.add(this._makeEndpoint(host, initial.port, initial.wsPort, initial.protocol).url);
                }
            }
            return { initial, protocol: endpoint.protocol, visited, hops: 0, metadata: null };
        }

        _redirectEndpoint(message, entry, flow) {
            const payload = message.payload;
            if (!payload || payload.protocol !== 'pool_redirect_v1' || message.client_id !== entry.context.clientId ||
                typeof entry.membershipPool !== 'string' || !entry.membershipPool ||
                message.pool !== entry.membershipPool || payload.pool !== entry.membershipPool ||
                !Number.isSafeInteger(payload.pod_count) || payload.pod_count <= 0 ||
                !Number.isSafeInteger(payload.pod_index) || payload.pod_index < 0 || payload.pod_index >= payload.pod_count ||
                typeof payload.cluster_id !== 'string' || !payload.cluster_id || payload.cluster_id.length > 512) {
                throw this._error('invalid_redirect', 'Invalid pool redirect identity, protocol or owner metadata');
            }
            const host = this._loopbackHost(payload.host);
            const routerHost = this._loopbackHost(payload.router_host);
            if (!host || !routerHost || !this._loopbackHost(flow.initial.host, true)) {
                throw this._error('unsafe_redirect', 'Pool redirects require a loopback entry and numeric loopback endpoints');
            }
            const validPort = port => Number.isInteger(port) && port >= 1 && port <= 65535;
            if (!validPort(payload.port) || !validPort(payload.router_port) ||
                (payload.ws_port !== null && !validPort(payload.ws_port)) ||
                (payload.router_ws_port !== null && !validPort(payload.router_ws_port))) {
                throw this._error('invalid_redirect', 'Invalid pool redirect port metadata');
            }
            if (payload.ws_port === null) throw this._error('redirect_unavailable', 'Owner has no WebSocket listener');
            const metadata = JSON.stringify([payload.cluster_id, payload.pod_count, routerHost,
                payload.router_port, payload.router_ws_port]);
            if (flow.metadata !== null && flow.metadata !== metadata) {
                throw this._error('invalid_redirect', 'Pool redirect cluster metadata changed within one operation');
            }
            if (this.maxRedirects === 0) throw this._error('redirect_required', 'Pool redirects are disabled');
            if (flow.hops >= this.maxRedirects) throw this._error('redirect_limit', 'Pool redirect hop limit exceeded');
            // Only the explicit WS port is followed. The reply cannot supply a URL or change the caller's scheme.
            const endpoint = this._makeEndpoint(host, payload.port, payload.ws_port, flow.protocol);
            if (flow.visited.has(endpoint.url)) throw this._error('redirect_cycle', 'Pool redirect endpoint was already visited');
            flow.metadata = metadata;
            flow.hops++;
            flow.visited.add(endpoint.url);
            return endpoint;
        }

        _connectionAttempt(deadline, pool, authToken, switchOperation = null) {
            let resolve, reject;
            const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
            const attempt = { promise, resolve, reject, deadline, pool, authToken, switchOperation, timer: null };
            this._connecting = attempt;
            attempt.timer = setTimeout(() => {
                if (this._connecting === attempt) {
                    this._closeConnection(this._error('timeout', 'Connection handshake timeout'));
                }
            }, Math.max(0, deadline - this._now()));
            return attempt;
        }

        _openConnection(attempt, endpoint) {
            if (this._connecting !== attempt || this._intentionalDisconnect) return;
            try {
                if (this._now() >= attempt.deadline) throw this._error('timeout', 'Connection handshake timeout');
                const ws = new WebSocket(endpoint.url);
                this.ws = ws;
                this._endpoint = endpoint;
                this._entryEndpoint = attempt.redirects.initial;
                this.connected = false;
                this._generation++;
                const context = this._context();

                ws.onopen = () => {
                    if (!this._isCurrent(context) || this._connecting !== attempt) return;
                    const options = { deadline: attempt.deadline, context, allowHandshake: true, allowSwitch: true };
                    this.sendRequest('hello', { capabilities: this.maxRedirects > 0 ? ['pool_redirect_v1'] : [] }, null, options)
                        .then(() => {
                            if (!this._isCurrent(context) || this._connecting !== attempt) {
                                throw this._error('connection_lost', 'Connection handshake was superseded');
                            }
                            return this.sendRequest('join_pool', {
                                client_id: context.clientId, pool: attempt.pool, auth_token: attempt.authToken
                            }, undefined, {
                                ...options,
                                onAck: () => this._completeConnection(attempt, context),
                                onRedirect: (message, entry) => {
                                    const target = this._redirectEndpoint(message, entry, attempt.redirects);
                                    this._retireSocket(context, attempt.switchOperation ? 'pool_switched' : 'connection_lost', message.request_id);
                                    this._openConnection(attempt, target);
                                }
                            });
                        })
                        .catch(error => this._closeConnection(error, ws));
                };
                ws.onmessage = event => {
                    if (this.ws === ws) this.handleMessage(event.data, this._context());
                };
                ws.onerror = () => {
                    this._closeConnection(this._error('connection_lost', 'WebSocket connection failed'), ws);
                };
                ws.onclose = () => {
                    this._closeConnection(this._error('connection_lost', 'WebSocket connection closed'), ws);
                };
            } catch (error) {
                this._closeConnection(error);
            }
        }

        _completeConnection(attempt, context) {
            if (!this._isCurrent(context) || this._connecting !== attempt || this._intentionalDisconnect) return;
            clearTimeout(attempt.timer);
            this._connecting = null;
            this.poolName = attempt.pool;
            this.authToken = attempt.authToken;
            this.connected = true;
            this.reconnectAttempts = 0;
            if (attempt.switchOperation === this._switchOperation) {
                this._switching = false;
                this._switchTransmitted = false;
            }
            attempt.resolve();
            this.dispatchEvent(new CustomEvent('connect'));
        }

        _retireSocket(context, code, skipRequestId) {
            this.connected = false;
            this.ws = null;
            this._endpoint = null;
            this._entryEndpoint = null;
            this._generation++;
            this._clearConnectionWork(this._error(code, 'Request cancelled by pool redirect'), skipRequestId);
            this._switchTransmitted = false;
            if (context.ws.readyState < WebSocket.CLOSING) {
                try { context.ws.close(); } catch (_) { /* Superseded socket callbacks are already fenced. */ }
            }
        }

        _now() {
            return performance.now();
        }

        _deadline(timeout = this.timeout) {
            if (!Number.isFinite(timeout) || timeout <= 0 || timeout > 2147483647) {
                throw new RangeError('timeout must be a positive finite millisecond duration <= 2147483647');
            }
            return this._now() + timeout;
        }

        _error(code, message) {
            const error = new Error(message);
            error.code = code;
            return error;
        }

        _context() {
            return { ws: this.ws, generation: this._generation,
                pool: this._connecting ? this._connecting.pool : this.poolName, clientId: this.clientId };
        }

        _isCurrent(context) {
            return !!context.ws && context.ws === this.ws && context.generation === this._generation &&
                context.pool === (this._connecting ? this._connecting.pool : this.poolName);
        }

        _requireSocket(context, allowHandshake = false) {
            if (!this._isCurrent(context) || this._intentionalDisconnect ||
                context.ws.readyState !== WebSocket.OPEN || (!this.connected && !allowHandshake)) {
                throw this._error('connection_lost', 'Not connected to server');
            }
        }

        _closeConnection(error, socket = this.ws, report = true) {
            if (socket && socket !== this.ws) return;
            const attempt = this._connecting;
            const hadConnection = !!this.ws || !!attempt || this.connected;
            this.connected = false;
            this.ws = null;
            this._endpoint = null;
            this._entryEndpoint = null;
            this._generation++;
            this._connecting = null;
            this._switchOperation = null;
            this._switching = false;
            this._switchTransmitted = false;
            if (attempt) {
                clearTimeout(attempt.timer);
                attempt.reject(error);
            }
            this._clearConnectionWork(error);
            if (socket && socket.readyState < WebSocket.CLOSING) {
                try { socket.close(); } catch (_) { /* The connection is already fenced locally. */ }
            }
            if (hadConnection) {
                if (report && !this._intentionalDisconnect) {
                    this.dispatchEvent(new CustomEvent('error', { detail: error }));
                }
                this.dispatchEvent(new CustomEvent('disconnect'));
            }
            this._scheduleReconnect();
        }

        _clearConnectionWork(error, skipRequestId = null) {
            clearTimeout(this._sendTimer);
            this._sendTimer = null;
            for (const frame of this._outbox) clearTimeout(frame.timer);
            this._outbox = [];
            this._queuedBytes = 0;
            for (const id of Array.from(this.pending.keys())) {
                if (id !== skipRequestId) this._settle(id, error);
            }
            for (const waiter of this._handlerWaiters) {
                clearTimeout(waiter.timer);
                waiter.reject(error);
            }
            this._handlerWaiters.clear();
            this._processes.clear();
            this._deferredNotifications = [];
            this._deferredNotificationBytes = 0;
        }

        _scheduleReconnect() {
            if (this._intentionalDisconnect || this.connected || this._connecting || this._reconnectTimer !== null ||
                this.reconnectAttempts >= this.maxReconnectAttempts) return;
            const base = Math.min(this.maxReconnectDelay, this.reconnectDelay * Math.pow(2, this.reconnectAttempts));
            const delay = Math.min(this.maxReconnectDelay, Math.max(1, base * (0.75 + Math.random() * 0.5)));
            this.reconnectAttempts++;
            this._reconnectTimer = setTimeout(() => {
                this._reconnectTimer = null;
                if (!this._intentionalDisconnect) this.connect().catch(() => {});
            }, delay);
        }

        handleMessage(data, context = this._context()) {
            try {
                if (typeof data !== 'string' || data.length > this.maxFrameBytes ||
                    new TextEncoder().encode(data).byteLength > this.maxFrameBytes) {
                    throw this._error('invalid_frame', 'Expected a bounded WebSocket JSON text frame');
                }
                const message = JSON.parse(data);
                this.handleMessageObject(message, context);
            } catch (error) {
                this._closeConnection(error, context.ws);
            }
        }

        handleMessageObject(message, context = this._context()) {
            if (!message || Array.isArray(message) || typeof message !== 'object' || typeof message.type !== 'string' ||
                (message.payload != null && (typeof message.payload !== 'object' || Array.isArray(message.payload)))) {
                throw this._error('invalid_frame', 'Invalid protocol message');
            }
            if (!this._isCurrent(context)) return;
            const { type, request_id, payload } = message;
            const entry = this.pending.get(request_id);
            const matches = entry && entry.sent && entry.context.ws === context.ws &&
                entry.context.generation === context.generation &&
                (message.pool == null || message.pool === entry.context.pool || entry.type === 'switch_pool');

            if (type === 'redirect') {
                if (!entry || !entry.sent || entry.context.ws !== context.ws ||
                    entry.context.generation !== context.generation ||
                    !['join_pool', 'switch_pool'].includes(entry.type) || !entry.onRedirect) return;
                try {
                    if (this._now() >= entry.deadline) throw this._error('timeout', 'Request timeout');
                    entry.onRedirect(message, entry);
                    this._settle(request_id, null, message);
                } catch (error) {
                    const failure = this._settle(request_id, error);
                    this._closeConnection(failure || error, context.ws);
                }
                return;
            }

            if (matches && (type === 'ack' || type === 'error' || (type === 'app_result' && entry.kind === 'result')) &&
                this._now() >= entry.deadline) {
                const error = this._error('timeout', 'Request timeout');
                const failure = this._settle(request_id, error);
                if (this._connecting && ['hello', 'join_pool'].includes(entry.type)) {
                    this._closeConnection(failure || error, context.ws);
                }
                if (type === 'app_result') this.handleServerMessage(message, context);
                return;
            }

            // Shared IDs never make pushes or callee invocations into replies.
            if (matches && type === 'error') {
                const error = this._error(payload?.code || 'server_error', payload?.message || 'Server error');
                if (entry.type === 'switch_pool') error.uncertain = false;
                const failure = this._settle(request_id, error);
                if (this._connecting && ['hello', 'join_pool'].includes(entry.type)) {
                    this._closeConnection(failure || error, context.ws);
                }
                return;
            }
            if (matches && type === 'ack') {
                if ((entry.type === 'call_app' || entry.type === 'call_process') && payload?.delivered && !payload?.queued) return;
                entry.ack = message;
                if (entry.kind === 'ack') {
                    if (entry.onAck) entry.onAck(message);
                    this._settle(request_id, null, message);
                }
                return;
            }
            if (matches && type === 'app_result' && entry.kind === 'result') {
                this._settle(request_id, null, message);
                return;
            }
            this.handleServerMessage(message, context);
        }

        handleServerMessage(message, context = this._context()) {
            const { type, payload } = message;
            if (message.pool != null && message.pool !== context.pool) return;
            if (this._switching && this._switchTransmitted &&
                ['presence_update', 'buffer_update', 'emit_event'].includes(type)) {
                const bytes = new TextEncoder().encode(JSON.stringify(message)).byteLength;
                if (this._deferredNotifications.length >= this.maxQueuedMessages ||
                    this._deferredNotificationBytes + bytes > this.maxQueuedBytes) {
                    this._closeConnection(this._error('overloaded', 'Pool-switch notification admission limit reached'), context.ws);
                } else {
                    this._deferredNotifications.push({ message, context });
                    this._deferredNotificationBytes += bytes;
                }
                return;
            }
            switch (type) {
                case 'presence_update':
                    if (this._activeHandlers.size >= this.maxActiveHandlers) {
                        this._closeConnection(this._error('overloaded', 'Presence handler admission is unavailable'), context.ws);
                        break;
                    }
                    this.dispatchEvent(new CustomEvent('presence', { detail: payload }));
                    break;
                case 'buffer_update':
                    if (this._activeHandlers.size >= this.maxActiveHandlers) {
                        this._closeConnection(this._error('overloaded', 'Buffer handler admission is unavailable'), context.ws);
                        break;
                    }
                    this.dispatchEvent(new CustomEvent('bufferUpdate', { detail: payload }));
                    break;
                case 'emit_event':
                    this.handleEventMessage(payload || {}, context);
                    break;
                case 'call_app':
                    this.handleAppCall(message, context).catch(error => {
                        if (this._isCurrent(context)) this._closeConnection(error, context.ws);
                    });
                    break;
                case 'call_process':
                    this.handleProcessCall(message, context).catch(error => {
                        if (this._isCurrent(context)) this._closeConnection(error, context.ws);
                    });
                    break;
                case 'app_result':
                    this.dispatchEvent(new CustomEvent('app_result', { detail: message }));
                    break;
                case 'error':
                    this.dispatchEvent(new CustomEvent('error', {
                        detail: this._error(payload?.code || 'server_error', payload?.message || 'Server error')
                    }));
                    break;
            }
        }

        _runHandler(work) {
            const token = {};
            this._activeHandlers.add(token);
            return Promise.resolve().then(work).finally(() => {
                this._activeHandlers.delete(token);
                if (this._activeHandlers.size === 0) {
                    for (const waiter of this._handlerWaiters) {
                        clearTimeout(waiter.timer);
                        waiter.resolve();
                    }
                    this._handlerWaiters.clear();
                }
            });
        }

        _drainHandlers(deadline) {
            if (this._activeHandlers.size === 0) return Promise.resolve();
            return new Promise((resolve, reject) => {
                const waiter = { resolve, reject, timer: null };
                waiter.timer = setTimeout(() => {
                    this._handlerWaiters.delete(waiter);
                    reject(this._error('timeout', 'Handler quiescence timeout'));
                }, Math.max(0, deadline - this._now()));
                this._handlerWaiters.add(waiter);
            });
        }

        handleEventMessage(payload, context = this._context()) {
            const { event, data } = payload;
            const handlers = (this.eventHandlers.get(event) || []).slice();
            if (handlers.length === 0) return;
            if (this._switching || this._activeHandlers.size >= this.maxActiveHandlers) {
                this._closeConnection(this._error('overloaded', 'Notification handler admission is unavailable'), context.ws);
                return;
            }
            this._runHandler(async () => {
                for (const handler of handlers) {
                    if (!this._isCurrent(context)) return;
                    try { await handler(data); }
                    catch (error) { this.dispatchEvent(new CustomEvent('error', { detail: error })); }
                }
            }).catch(error => this._closeConnection(error, context.ws));
        }

        handleAppCall(message, context = this._context()) {
            return this._handleInvocation(message.payload?.event, message, context);
        }

        handleProcessCall(message, context = this._context()) {
            return this._handleInvocation(message.payload?.process_id, message, context);
        }

        _applicationError(error) {
            let type = 'Error', message = 'Application handler failed';
            try {
                if (typeof error?.name === 'string') type = error.name.slice(0, 128);
                if (typeof error?.message === 'string') message = error.message.slice(0, 512);
                else if (typeof error === 'string') message = error.slice(0, 512);
            } catch (_) { /* Even thrown proxies must have a JSON-safe error reply. */ }
            return { value: null, error: { type, message } };
        }

        async _handleInvocation(event, message, context) {
            if (!this._isCurrent(context) || !this.connected) return;
            const prefix = `${context.clientId}:`;
            const record = typeof event === 'string' && event.startsWith(prefix) ?
                this._processes.get(event.slice(prefix.length)) : null;
            const handler = record ? (record.context.generation === context.generation &&
                record.context.ws === context.ws && record.context.pool === context.pool ? record.fn : null) :
                this.eventHandlers.get(event)?.[0];
            const reply = {
                type: 'app_result', request_id: message.request_id,
                client_id: context.clientId, pool: message.pool ?? context.pool
            };
            const sendError = (type, text) => {
                this._enqueueMessage({ ...reply, payload: { value: null, error: { type, message: text } } },
                    context, this._deadline(), { control: true });
            };
            if (this._switching) {
                sendError('PoolSwitching', 'Handler admission paused for pool switch');
                return;
            }
            if (this._activeHandlers.size >= this.maxActiveHandlers) {
                sendError('Overloaded', 'Handler admission limit reached');
                return;
            }
            if (!handler) {
                sendError('NoHandler', `No handler registered for '${String(event).slice(0, 128)}'`);
                return;
            }
            await this._runHandler(async () => {
                if (!this._isCurrent(context)) return;
                let encoded;
                try {
                    const value = await handler(message.payload?.data);
                    // Snapshot once inside the boundary, including custom toJSON and non-JSON primitives.
                    const json = JSON.stringify(value === undefined ? null : value);
                    if (json === undefined) throw new TypeError('Handler result is not JSON-serializable');
                    encoded = this._serialize({ ...reply, payload: { value: JSON.parse(json), error: null } });
                } catch (error) {
                    encoded = this._serialize({ ...reply, payload: this._applicationError(error) });
                }
                if (this._isCurrent(context) && this.connected) {
                    this._enqueueEncoded(encoded, context, this._deadline(), { control: true });
                }
            });
        }

        _serialize(message) {
            const json = JSON.stringify(message);
            if (typeof json !== 'string') throw new TypeError('Message is not JSON-serializable');
            const bytes = new TextEncoder().encode(json).byteLength;
            if (bytes > this.maxFrameBytes || bytes > this.maxBufferedAmount) {
                throw this._error('frame_too_large', 'Serialized message exceeds the frame/send budget');
            }
            return { json, bytes };
        }

        _enqueueMessage(message, context, deadline, options = {}) {
            const rpc = ['call_app', 'call_process', 'broadcast_process'].includes(message.type);
            return this._enqueueEncoded(this._serialize(message), context, deadline, { ...options, rpc });
        }

        _enqueueEncoded(encoded, context, deadline, options = {}) {
            this._requireSocket(context, options.allowHandshake);
            const remaining = deadline - this._now();
            if (remaining <= 0) throw this._error('timeout', 'Request deadline exceeded before send');
            const messageLimit = this.maxQueuedMessages + (options.control ? this.maxActiveHandlers : 0);
            const byteLimit = this.maxQueuedBytes + (options.control ? this.maxFrameBytes : 0);
            if (this._outbox.length >= messageLimit || this._queuedBytes + encoded.bytes > byteLimit) {
                throw this._error('overloaded', 'WebSocket send queue admission limit reached');
            }
            const frame = { ...encoded, ...options, context, deadline, timer: null };
            this._outbox.push(frame);
            this._queuedBytes += frame.bytes;
            if (!frame.pendingId) {
                frame.timer = setTimeout(() => {
                    if (this._outbox.includes(frame)) {
                        this._closeConnection(this._error('timeout', 'Queued reply/send deadline exceeded'), context.ws);
                    }
                }, remaining);
            }
            this._flushOutbox();
        }

        _flushOutbox() {
            if (this._flushing) return;
            this._flushing = true;
            try {
                while (this._outbox.length) {
                    const frame = this._outbox[0];
                    if (this._switching && this._switchTransmitted) return;
                    try {
                        this._requireSocket(frame.context, frame.allowHandshake);
                        const remaining = frame.deadline - this._now();
                        if (remaining <= 0) throw this._error('timeout', 'Request deadline exceeded before send');
                        let encoded = frame;
                        if (frame.rpc) {
                            const message = JSON.parse(frame.json);
                            message.payload.timeout = remaining / 1000;
                            encoded = this._serialize(message);
                        }
                        if (frame.context.ws.bufferedAmount + encoded.bytes > this.maxBufferedAmount) {
                            if (this._sendTimer === null) {
                                this._sendTimer = setTimeout(() => {
                                    this._sendTimer = null;
                                    this._flushOutbox();
                                }, this.sendPollInterval);
                            }
                            return;
                        }
                        this._outbox.shift();
                        this._queuedBytes -= frame.bytes;
                        clearTimeout(frame.timer);
                        if (frame.pendingId) {
                            const entry = this.pending.get(frame.pendingId);
                            if (!entry) continue;
                            entry.sent = true;
                        }
                        if (frame.onSend) frame.onSend();
                        try { frame.context.ws.send(encoded.json); }
                        catch (error) {
                            this._closeConnection(this._error('connection_lost', error.message || 'WebSocket send failed'),
                                frame.context.ws);
                        }
                    } catch (error) {
                        if (frame.pendingId) this._settle(frame.pendingId, error);
                        else { this._closeConnection(error, frame.context.ws); return; }
                    }
                }
                clearTimeout(this._sendTimer);
                this._sendTimer = null;
            } finally {
                this._flushing = false;
            }
        }

        sendMessage(message, context = this._context()) {
            this._enqueueMessage(message, context, this._deadline(), { control: message.type === 'app_result' });
        }

        _settle(requestId, error, message) {
            const entry = this.pending.get(requestId);
            if (!entry) return;
            this.pending.delete(requestId);
            clearTimeout(entry.timer);
            this._outbox = this._outbox.filter(frame => {
                if (frame.pendingId !== requestId) return true;
                this._queuedBytes -= frame.bytes;
                clearTimeout(frame.timer);
                return false;
            });
            if (this._outbox.length === 0) {
                clearTimeout(this._sendTimer);
                this._sendTimer = null;
            }
            if (error) {
                // A sent request can have had effects; cancellation never replays it.
                const failure = this._error(error.code || 'request_failed', error.message || 'Request failed');
                failure.request_id = requestId;
                failure.uncertain = error.uncertain ?? entry.sent;
                entry.reject(failure);
                return failure;
            } else entry.resolve(message);
        }

        sendRequest(type, payload, pool = undefined, options = {}) {
            return new Promise((resolve, reject) => {
                let requestId, entry;
                try {
                    const deadline = options.deadline ?? this._deadline(options.timeout ?? this.timeout);
                    if (!Number.isFinite(deadline) || deadline - this._now() > 2147483647) {
                        throw new RangeError('Request deadline must be finite and within 2147483647 milliseconds');
                    }
                    const context = options.context || this._context();
                    this._requireSocket(context, options.allowHandshake);
                    if (this._switching && !options.allowSwitch) {
                        throw this._error('pool_switch_in_progress', 'Requests are paused for pool switch');
                    }
                    if (this.pending.size >= this.maxPendingRequests) {
                        throw this._error('overloaded', 'Pending request admission limit reached');
                    }
                    requestId = this.generateRequestId();
                    const rpc = type === 'call_app' || type === 'call_process';
                    const kind = rpc && (!payload?.response_to || payload.response_to === this.clientId) ? 'result' : 'ack';
                    entry = { resolve, reject, type, kind, context, deadline, sent: false, timer: null,
                        onAck: options.onAck, onRedirect: options.onRedirect, membershipPool: payload?.pool };
                    this.pending.set(requestId, entry);
                    entry.timer = setTimeout(() => this._settle(requestId,
                        this._error('timeout', 'Request timeout')), Math.max(0, deadline - this._now()));
                    this._enqueueMessage({
                        type, request_id: requestId, client_id: context.clientId,
                        pool: pool === undefined ? context.pool : pool, payload: payload ?? {}
                    }, context, deadline, {
                        pendingId: requestId, allowHandshake: options.allowHandshake, onSend: options.onSend
                    });
                } catch (error) {
                    if (entry) this._settle(requestId, error);
                    else reject(error);
                }
            });
        }
        
        generateRequestId() {
            return 'req_' + Date.now() + '_' + (++this._requestSequence) + '_' + Math.random().toString(36).slice(2, 11);
        }
        
        // Core API methods
        async set(key, value, options = {}) {
            this.ensureJsonable(value);
            if (options.autoClean != null && (!Number.isFinite(options.autoClean) || options.autoClean < 0)) {
                throw new RangeError('autoClean must be nonnegative finite milliseconds');
            }
            await this.sendRequest('set_buffer', {
                key,
                value,
                ttl: options.autoClean == null ? options.autoClean : options.autoClean / 1000,
                persistent: options.persistent || false
            }, undefined, { timeout: options.timeout ?? this.timeout });
        }
        
        async get(key, defaultValue = null) {
            const response = await this.sendRequest('get_buffer', { key });
            const payload = response.payload || {};
            if (!payload.exists) {
                return defaultValue;
            }
            return payload.entry && Object.prototype.hasOwnProperty.call(payload.entry, 'value') ?
                payload.entry.value : defaultValue;
        }
        
        async delete(key) {
            const response = await this.sendRequest('delete_buffer', { key });
            return !!(response.payload?.deleted);
        }
        
        async exists(key) {
            const response = await this.sendRequest('get_buffer', { key });
            return !!(response.payload?.exists);
        }
        
        async keys(pattern = null) {
            const response = await this.sendRequest('list_buffers', { pattern });
            return response.payload?.keys || [];
        }
        
        async values(pattern = null) {
            return this._batch(async () => {
                const keys = await this.keys(pattern);
                this._checkBatchSize(keys.length);
                return keys.map(key => this.get(key));
            });
        }
        
        async items(pattern = null) {
            return this._batch(async () => {
                const keys = await this.keys(pattern);
                this._checkBatchSize(keys.length);
                return keys.map(async key => [key, await this.get(key)]);
            });
        }
        
        async mset(data, options = {}) {
            const entries = Object.entries(data);
            this._checkBatchSize(entries.length);
            entries.forEach(([, value]) => this.ensureJsonable(value));
            await this._batch(() => entries.map(([key, value]) => this.set(key, value, options)));
        }
        
        async mget(keys) {
            this._checkBatchSize(keys.length);
            return this._batch(() => keys.map(key => this.get(key))).then(values => Object.fromEntries(
                keys.map((key, i) => [key, values[i]])
            ));
        }
        
        async deleteMany(keys) {
            this._checkBatchSize(keys.length);
            const results = await this._batch(() => keys.map(key => this.delete(key)));
            return results.filter(Boolean).length;
        }

        _checkBatchSize(size) {
            if (!Number.isSafeInteger(size) || size < 0 || size > this.maxBatchSize) {
                throw this._error('overloaded', 'Batch item admission limit reached');
            }
        }

        async _batch(work) {
            if (this._activeBatches >= this.maxPendingBatches) {
                throw this._error('overloaded', 'Pending batch admission limit reached');
            }
            this._activeBatches++;
            try {
                const result = await work();
                if (!Array.isArray(result)) return result;
                // Keep the slot until every started item settles, even on partial failure.
                const settled = await Promise.all(result.map(promise => Promise.resolve(promise).then(
                    value => ({ value }), error => ({ error })
                )));
                const failed = settled.find(item => Object.prototype.hasOwnProperty.call(item, 'error'));
                if (failed) throw failed.error;
                return settled.map(item => item.value);
            } finally {
                this._activeBatches--;
            }
        }
        
        async size() {
            const keys = await this.keys();
            return keys.length;
        }
        
        async stats() {
            return {
                name: this.poolName,
                client_id: this.clientId,
                server_mode: true,
                key_count: await this.size()
            };
        }
        
        async scan(cursor = 0, count = 100) {
            const keys = await this.keys();
            const end = Math.min(cursor + count, keys.length);
            const nextCursor = end < keys.length ? end : 0;
            return [nextCursor, keys.slice(cursor, end)];
        }
        
        // Event methods
        on(event, handler) {
            if (typeof handler !== 'function') throw new TypeError('Event handler must be a function');
            if (!this.eventHandlers.has(event)) {
                this.eventHandlers.set(event, []);
            }
            this.eventHandlers.get(event).push(handler);
        }
        
        off(event, handler) {
            if (!this.eventHandlers.has(event)) return;
            const handlers = this.eventHandlers.get(event);
            const index = handlers.indexOf(handler);
            if (index > -1) {
                handlers.splice(index, 1);
            }
            if (handlers.length === 0) {
                this.eventHandlers.delete(event);
            }
        }
        
        async emitEvent(event, options = {}) {
            this.ensureJsonable(options.data || {});
            await this.sendRequest('emit_event', {
                event,
                data: options.data || {},
                target_client_id: options.targetClientId || null,
                response_to: options.responseTo || null
            });
        }
        
        async callEvent(event, options = {}) {
            if (!options.targetClientId) {
                throw new Error('callEvent requires targetClientId');
            }
            
            const timeoutMs = options.timeout ?? this.timeout;
            return this.sendRequest('call_app', {
                target_client_id: options.targetClientId,
                event,
                data: options.data || {},
                response_to: options.responseTo || null,
                timeout: timeoutMs / 1000
            }, undefined, { timeout: timeoutMs });
        }
        
        // Utility methods
        ensureJsonable(value) {
            try {
                if (JSON.stringify(value) === undefined) throw new TypeError('Missing JSON value');
            } catch (err) {
                throw new TypeError('Server mode only supports JSON-serializable values');
            }
        }
        
        async switchPool(pool, authToken = null) {
            if (this._switching) throw this._error('pool_switch_in_progress', 'Pool switch already in progress');
            if (typeof pool !== 'string' || !pool) throw new TypeError('Pool must be a nonempty string');
            const context = this._context();
            this._requireSocket(context);
            const deadline = this._deadline();
            const operation = {};
            this._switchOperation = operation;
            this._switching = true;
            this._switchTransmitted = false;
            this._deferredNotifications = [];
            this._deferredNotificationBytes = 0;
            let redirected = null;
            try {
                await this._drainHandlers(deadline);
                if (this._switchOperation !== operation || !this._isCurrent(context)) {
                    throw this._error('connection_lost', 'Pool switch was cancelled');
                }
                // Flush replies accepted before quiescence; the switch remains ordered behind them.
                await this.sendRequest('switch_pool', {
                    client_id: this.clientId, pool, auth_token: authToken ?? this.authToken
                }, undefined, {
                    context, deadline, allowSwitch: true,
                    onSend: () => { this._switchTransmitted = true; },
                    onRedirect: (message, entry) => {
                        const flow = this._redirectFlow(this._endpoint, this._entryEndpoint);
                        const target = this._redirectEndpoint(message, entry, flow);
                        redirected = this._connectionAttempt(deadline, pool, authToken ?? this.authToken, operation);
                        redirected.redirects = flow;
                        redirected.requestId = message.request_id;
                        redirected.promise.catch(() => {});
                        this._retireSocket(context, 'pool_switched', message.request_id);
                        this.eventHandlers.clear();
                        this._openConnection(redirected, target);
                    },
                    onAck: () => {
                        this.poolName = pool;
                        this.authToken = authToken ?? this.authToken;
                        this._switching = false;
                        if (pool === context.pool) return; // Idempotent same-pool rejoin retains registrations/routes.
                        this._generation++;
                        this._processes.clear();
                        this.eventHandlers.clear();
                        this._deferredNotifications = []; // Only a successful membership change discards old-pool pushes.
                        this._deferredNotificationBytes = 0;
                        for (const [id, entry] of Array.from(this.pending)) {
                            if (entry.type !== 'switch_pool') {
                                this._settle(id, this._error('pool_switched', 'Request cancelled by pool switch'));
                            }
                        }
                        // Successful switching finalizes old routes at the daemon.
                        this._outbox = this._outbox.filter(frame => {
                            if (frame.context.generation !== context.generation) return true;
                            this._queuedBytes -= frame.bytes;
                            clearTimeout(frame.timer);
                            return false;
                        });
                        if (this._outbox.length === 0) {
                            clearTimeout(this._sendTimer);
                            this._sendTimer = null;
                        }
                    }
                });
                if (redirected) await redirected.promise;
            } catch (error) {
                if (redirected && error.request_id === undefined) {
                    const failure = this._error(error.code || 'send_failed', error.message || String(error));
                    failure.request_id = redirected.requestId;
                    failure.uncertain = true;
                    error = failure;
                }
                if (this._switchOperation === operation && this._switchTransmitted &&
                    error.code !== 'pool_switched' && error.code !== 'connection_lost' &&
                    error.uncertain !== false) {
                    // The switch may have committed remotely; do not continue with ambiguous membership.
                    this._closeConnection(error, context.ws);
                }
                throw error;
            } finally {
                if (this._switchOperation === operation) {
                    this._switchOperation = null;
                    this._switching = false;
                    this._switchTransmitted = false;
                    this._flushOutbox();
                    const notifications = this._deferredNotifications;
                    this._deferredNotifications = [];
                    this._deferredNotificationBytes = 0;
                    for (const item of notifications) {
                        if (this._isCurrent(item.context)) this.handleServerMessage(item.message, item.context);
                    }
                }
            }
        }

        disconnect() {
            this._intentionalDisconnect = true;
            clearTimeout(this._reconnectTimer);
            this._reconnectTimer = null;
            // Closing the socket removes membership without an unobserved leave request.
            this._closeConnection(this._error('connection_lost', 'Client intentionally disconnected'), this.ws, false);
        }
    }

    // Export to global scope
    global.LatZeroWebClient = LatZeroWebClient;

})(typeof window !== 'undefined' ? window : global);
