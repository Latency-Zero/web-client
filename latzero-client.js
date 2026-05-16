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
            this.port = options.port || 14130;
            this.timeout = options.timeout || 5000;
            
            // Connection state
            this.ws = null;
            this.connected = false;
            this.pending = new Map(); // request_id -> resolve/reject
            this.eventHandlers = new Map(); // event -> [handlers]
            this.reconnectAttempts = 0;
            this.maxReconnectAttempts = options.maxReconnectAttempts || 5;
            this.reconnectDelay = options.reconnectDelay || 1000;
            
            // Process pool state
            this._processes = new Map(); // short process name -> fn
            
            // Process pool proxy — all process operations live under client.process.*
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
                register: async (fn, nameOverride = null) => {
                    const name = nameOverride || fn.name;
                    if (!name) {
                        throw new Error(
                            'Anonymous functions must have an explicit name: register(fn, "name")'
                        );
                    }

                    const compoundKey = `${_self.clientId}:${name}`;
                    if (!_self.eventHandlers.has(compoundKey)) {
                        _self.eventHandlers.set(compoundKey, []);
                    }
                    _self.eventHandlers.get(compoundKey).push(fn);
                    _self._processes.set(name, fn);
                    await _self.sendRequest('register_process', { process_name: name });
                },

                /**
                 * Unregister a process by its short name.
                 */
                unregister: async (name) => {
                    const compoundKey = `${_self.clientId}:${name}`;
                    _self.eventHandlers.delete(compoundKey);
                    _self._processes.delete(name);
                    await _self.sendRequest('unregister_process', { process_name: name });
                },

                /**
                 * Call a process by its full process_id.
                 * process_id format: "client_id:process_name"
                 */
                call: async (processId, data = {}, options = {}) => {
                    const timeoutMs = options.timeout || _self.timeout;

                    if (options.responseTo) {
                        return _self.sendRequest('call_process', {
                            process_id: processId,
                            data,
                            response_to: options.responseTo,
                            timeout: timeoutMs / 1000
                        });
                    }

                    const requestId = _self.generateRequestId();
                    return new Promise((resolve, reject) => {
                        _self.pending.set(requestId, { resolve, reject });

                        try {
                            _self.sendMessage({
                                type: 'call_process',
                                request_id: requestId,
                                client_id: _self.clientId,
                                pool: _self.poolName,
                                payload: {
                                    process_id: processId,
                                    data,
                                    timeout: timeoutMs / 1000
                                }
                            });
                        } catch (err) {
                            _self.pending.delete(requestId);
                            reject(err);
                            return;
                        }

                        setTimeout(() => {
                            if (_self.pending.has(requestId)) {
                                _self.pending.delete(requestId);
                                reject(new Error('Process call timeout'));
                            }
                        }, timeoutMs);
                    });
                },

                /**
                 * Broadcast to all processes registered under the given short name.
                 * Returns the list of process_ids that were invoked.
                 */
                broadcast: async (processName, data = {}, options = {}) => {
                    const response = await _self.sendRequest('broadcast_process', {
                        process_name: processName,
                        data,
                        timeout: (options.timeout || _self.timeout) / 1000
                    });
                    return response.payload?.invoked_processes || [];
                },

                /**
                 * List all registered processes in the pool.
                 * pattern optionally filters by client_id prefix.
                 */
                list: async (pattern = null) => {
                    const response = await _self.sendRequest('list_processes', { pattern });
                    return response.payload?.processes || [];
                }
            };
            
            // Auto-connect if not disabled
            if (options.autoConnect !== false) {
                this.connect();
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
            return new Promise((resolve, reject) => {
                if (this.connected) {
                    resolve();
                    return;
                }
                
                // Connect to WebSocket server (port + 1)
                const wsPort = this.port + 1;
                const wsUrl = `ws://${this.host}:${wsPort}`;
                const ws = new WebSocket(wsUrl);
                this.ws = ws;
                
                ws.onopen = () => {
                    console.log(`[LatZero] Connected to WebSocket server at ${this.host}:${wsPort}`);
                    this.connected = true;
                    this.reconnectAttempts = 0;
                    
                    // Start handshake
                    this.sendRequest('hello', null, null)
                        .then(() => this.sendRequest('join_pool', {
                            client_id: this.clientId,
                            pool: this.poolName,
                            auth_token: this.authToken
                        }))
                        .then(() => {
                            this.dispatchEvent(new CustomEvent('connect'));
                            resolve();
                        })
                        .catch(reject);
                };
                
                ws.onmessage = (event) => {
                    this.handleMessage(event.data);
                };
                
                ws.onerror = (error) => {
                    console.error('[LatZero] WebSocket error:', error);
                    this.connected = false;
                    this.dispatchEvent(new CustomEvent('error', { detail: error }));
                    reject(error);
                };
                
                ws.onclose = () => {
                    console.log('[LatZero] WebSocket connection closed');
                    this.connected = false;
                    this.dispatchEvent(new CustomEvent('disconnect'));
                    
                    // Auto-reconnect
                    if (this.reconnectAttempts < this.maxReconnectAttempts) {
                        this.reconnectAttempts++;
                        setTimeout(() => {
                            console.log(`[LatZero] Reconnecting... (${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
                            this.connect();
                        }, this.reconnectDelay);
                    }
                };
            });
        }
        
        handleMessage(data) {
            try {
                const message = JSON.parse(data);
                this.handleMessageObject(message);
            } catch (err) {
                console.error('[LatZero] Failed to parse message:', data, err);
            }
        }
        
        handleMessageObject(message) {
            const { type, request_id, client_id, pool, payload } = message;
            
            // Handle responses to pending requests
            if (request_id && this.pending.has(request_id)) {
                const { resolve, reject } = this.pending.get(request_id);
                this.pending.delete(request_id);
                
                if (type === 'ack') {
                    resolve({ type, payload });
                } else if (type === 'error') {
                    const error = new Error(payload?.message || 'Server error');
                    error.code = payload?.code || 'server_error';
                    reject(error);
                } else if (type === 'app_result') {
                    resolve({ type, payload });
                }
            } else {
                // Handle server-sent events
                this.handleServerMessage(message);
            }
        }
        
        handleServerMessage(message) {
            const { type, payload } = message;
            
            switch (type) {
                case 'presence_update':
                    this.dispatchEvent(new CustomEvent('presence', { detail: payload }));
                    break;
                case 'buffer_update':
                    this.dispatchEvent(new CustomEvent('bufferUpdate', { detail: payload }));
                    break;
                case 'emit_event':
                    this.handleEventMessage(payload);
                    break;
                case 'call_app':
                    this.handleAppCall(message);
                    break;
                case 'call_process':
                    this.handleProcessCall(message);
                    break;
            }
        }
        
        handleEventMessage(payload) {
            const { event, data } = payload;
            const handlers = this.eventHandlers.get(event) || [];
            handlers.forEach(handler => {
                try {
                    handler(data);
                } catch (err) {
                    console.error('[LatZero] Event handler error:', err);
                }
            });
        }
        
        handleAppCall(message) {
            const { request_id, payload } = message;
            const { event, data } = payload;
            
            // Check for handlers - the event should match the compound key format used for process registration
            const handlers = this.eventHandlers.get(event) || [];
            
            if (handlers.length === 0) {
                this.sendMessage({
                    type: 'app_result',
                    request_id,
                    client_id: this.clientId,
                    pool: this.poolName,
                    payload: {
                        value: null,
                        error: { type: 'NoHandler', message: `No handlers registered for '${event}'` }
                    }
                });
                return;
            }
            
            // Execute first handler and send result
            const handler = handlers[0];
            try {
                const result = handler(data);
                this.sendMessage({
                    type: 'app_result',
                    request_id,
                    client_id: this.clientId,
                    pool: this.poolName,
                    payload: {
                        value: result,
                        error: null
                    }
                });
            } catch (err) {
                this.sendMessage({
                    type: 'app_result',
                    request_id,
                    client_id: this.clientId,
                    pool: this.poolName,
                    payload: {
                        value: null,
                        error: { type: err.constructor.name, message: err.message }
                    }
                });
            }
        }
        
        handleProcessCall(message) {
            const { request_id, payload } = message;
            const { process_id, data } = payload;
            
            const [clientId, processName] = process_id.split(':');
            const compoundKey = `${clientId}:${processName}`;
            const handlers = this.eventHandlers.get(compoundKey) || [];
            
            if (handlers.length === 0) {
                this.sendMessage({
                    type: 'app_result',
                    request_id,
                    client_id: this.clientId,
                    pool: this.poolName,
                    payload: {
                        value: null,
                        error: { type: 'NoHandler', message: `No handler registered for '${processName}'` }
                    }
                });
                return;
            }
            
            // Execute first handler and send result
            const handler = handlers[0];
            try {
                const result = handler(data);
                this.sendMessage({
                    type: 'app_result',
                    request_id,
                    client_id: this.clientId,
                    pool: this.poolName,
                    payload: {
                        value: result,
                        error: null
                    }
                });
            } catch (err) {
                this.sendMessage({
                    type: 'app_result',
                    request_id,
                    client_id: this.clientId,
                    pool: this.poolName,
                    payload: {
                        value: null,
                        error: { type: err.constructor.name, message: err.message }
                    }
                });
            }
        }
        
        sendMessage(message) {
            if (!this.connected || !this.ws) {
                throw new Error('Not connected to server');
            }
            
            const json = JSON.stringify(message);
            this.ws.send(json);
        }
        
        sendRequest(type, payload, pool = null) {
            return new Promise((resolve, reject) => {
                const requestId = this.generateRequestId();
                this.pending.set(requestId, { resolve, reject });
                
                this.sendMessage({
                    type,
                    request_id: requestId,
                    client_id: this.clientId,
                    pool: pool || this.poolName,
                    payload: payload || {}
                });
                
                // Set timeout
                setTimeout(() => {
                    if (this.pending.has(requestId)) {
                        this.pending.delete(requestId);
                        reject(new Error('Request timeout'));
                    }
                }, this.timeout);
            });
        }
        
        generateRequestId() {
            return 'req_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
        }
        
        // Core API methods
        async set(key, value, options = {}) {
            this.ensureJsonable(value);
            await this.sendRequest('set_buffer', {
                key,
                value,
                ttl: options.autoClean,
                persistent: options.persistent || false
            });
        }
        
        async get(key, defaultValue = null) {
            const response = await this.sendRequest('get_buffer', { key });
            const payload = response.payload || {};
            if (!payload.exists) {
                return defaultValue;
            }
            return payload.entry?.value || defaultValue;
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
            const keys = await this.keys(pattern);
            const promises = keys.map(key => this.get(key));
            return await Promise.all(promises);
        }
        
        async items(pattern = null) {
            const keys = await this.keys(pattern);
            const promises = keys.map(async key => [key, await this.get(key)]);
            return await Promise.all(promises);
        }
        
        async mset(data, options = {}) {
            const promises = Object.entries(data).map(([key, value]) => 
                this.set(key, value, options)
            );
            await Promise.all(promises);
        }
        
        async mget(keys) {
            const promises = keys.map(key => this.get(key));
            const values = await Promise.all(promises);
            const result = {};
            keys.forEach((key, i) => result[key] = values[i]);
            return result;
        }
        
        async deleteMany(keys) {
            const promises = keys.map(key => this.delete(key));
            const results = await Promise.all(promises);
            return results.filter(Boolean).length;
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
            
            const requestId = this.generateRequestId();
            await this.sendRequest('call_app', {
                target_client_id: options.targetClientId,
                event,
                data: options.data || {},
                response_to: options.responseTo || null,
                timeout: options.timeout || this.timeout / 1000
            });
            
            // Wait for app_result response
            return new Promise((resolve, reject) => {
                const checkResult = () => {
                    const pending = Array.from(this.pending.entries());
                    const resultEntry = pending.find(([id, { resolve }]) => 
                        id.startsWith('app_result_') && resolve.toString().includes(event)
                    );
                    if (resultEntry) {
                        const [id, { resolve: res }] = resultEntry;
                        this.pending.delete(id);
                        res(resolve);
                    } else {
                        setTimeout(checkResult, 10);
                    }
                };
                checkResult();
            });
        }
        
        // Utility methods
        ensureJsonable(value) {
            try {
                JSON.stringify(value);
            } catch (err) {
                throw new TypeError('Server mode only supports JSON-serializable values');
            }
        }
        
        async switchPool(pool, authToken = null) {
            await this.sendRequest('switch_pool', {
                client_id: this.clientId,
                pool,
                auth_token: authToken || this.authToken
            });
            this.poolName = pool;
            this.authToken = authToken;
        }
        
        disconnect() {
            if (this.connected) {
                this.sendRequest('leave_pool', {});
            }
            if (this.ws) {
                this.ws.close();
            }
            this.connected = false;
        }
    }

    // Export to global scope
    global.LatZeroWebClient = LatZeroWebClient;

})(typeof window !== 'undefined' ? window : global);
