/* Opt-in external fixture only: this test never starts a server or chooses default ports/data. */
'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { randomUUID } = require('node:crypto');
const test = require('node:test');

test('native WS follows a disposable router, routes full results and switches owners without replay', {
    timeout: 20000,
    skip: !process.env.LATZERO_WEB_POD_TEST || typeof WebSocket !== 'function'
}, async t => {
    const config = JSON.parse(process.env.LATZERO_WEB_POD_TEST);
    assert.ok(['127.0.0.1', 'localhost', '::1', '[::1]'].includes(config.host ?? '127.0.0.1'));
    for (const key of ['port', 'wsPort']) {
        assert.ok(Number.isInteger(config[key]) && config[key] > 0 && config[key] <= 65535,
            `${key} must be the fixture's explicit bound port`);
        assert.ok(![14130, 14131].includes(config[key]), 'Refusing default production ports');
    }
    assert.equal(typeof config.pool, 'string');
    assert.equal(typeof config.switchPool, 'string');
    assert.ok(config.pool && config.switchPool && config.pool !== config.switchPool);
    const timeout = config.timeout ?? 5000;
    assert.ok(Number.isFinite(timeout) && timeout > 0 && timeout <= 10000);

    const source = fs.readFileSync(path.join(__dirname, 'latzero-client.js'), 'utf8');
    const NativeWebSocket = WebSocket;
    const opened = [];
    class TrackedWebSocket extends NativeWebSocket {
        constructor(url) {
            super(url);
            opened.push(String(url));
        }
    }
    class BrowserCustomEvent extends Event {
        constructor(type, options = {}) {
            super(type);
            this.detail = options.detail;
        }
    }
    const window = {};
    vm.runInNewContext(source, {
        window, URL, WebSocket: TrackedWebSocket, EventTarget, CustomEvent: BrowserCustomEvent,
        TextEncoder, performance, setTimeout, clearTimeout, console
    }, { filename: 'latzero-client.js' });
    const options = {
        host: config.host ?? '127.0.0.1', port: config.port, wsPort: config.wsPort,
        authToken: config.authToken ?? null, timeout, autoConnect: false, maxReconnectAttempts: 0
    };
    const id = `web-pods-${randomUUID()}`;
    const clients = ['origin', 'callee', 'recipient'].map(role =>
        new window.LatZeroWebClient(`latzero://${id}-${role}`, config.pool, options));
    const [origin, callee, recipient] = clients;
    const plain = value => JSON.parse(JSON.stringify(value));
    const key = `${id}-buffer`;
    let releaseHandler;
    t.after(() => {
        releaseHandler?.();
        for (const client of clients) client.disconnect();
    });

    function event(client, type, matches) {
        let timer, settled = false;
        let resolve, reject;
        const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
        const cleanup = () => {
            clearTimeout(timer);
            client.removeEventListener(type, listener);
            t.signal.removeEventListener('abort', abort);
        };
        const finish = (error, detail) => {
            if (settled) return;
            settled = true;
            cleanup();
            if (error) reject(error);
            else resolve(detail);
        };
        const listener = message => {
            if (matches(message.detail)) finish(null, message.detail);
        };
        const abort = () => finish(new Error('Integration event wait cancelled'));
        client.addEventListener(type, listener);
        t.signal.addEventListener('abort', abort, { once: true });
        t.after(() => finish(new Error('Integration event wait ended')));
        timer = setTimeout(() => finish(new Error(`No ${type} event before fixture deadline`)), timeout);
        promise.catch(() => {});
        return promise;
    }

    await Promise.all(clients.map(client => client.connect()));
    const firstOwner = origin.endpoint;
    assert.notEqual(firstOwner.wsPort, config.wsPort, 'The fixture must be a public pod router, not a classic server');
    assert.ok(clients.every(client => client.endpoint.url === firstOwner.url));
    assert.ok(clients.every(client => client.host === options.host && client.port === config.port && client.wsPort === config.wsPort));
    assert.ok(opened.some(url => Number(new URL(url).port) === config.wsPort));

    const value = { token: id, value: [0, false, null, { nested: 'exact' }] };
    await origin.set(key, value);
    assert.deepEqual(plain(await callee.get(key)), value);
    await recipient.sendRequest('subscribe', { key });
    const updated = event(recipient, 'bufferUpdate', payload => payload.key === key);
    await origin.set(key, { ...value, changed: true });
    assert.equal((await updated).key, key);

    callee.on('echo-app', data => data);
    await callee.process.register(data => data, 'echo');
    const direct = await origin.callEvent('echo-app', { targetClientId: callee.clientId, data: value });
    assert.equal(direct.type, 'app_result');
    assert.deepEqual(plain(direct.payload.value), value);
    const processReply = await origin.process.call(`${callee.clientId}:echo`, value);
    assert.equal(processReply.type, 'app_result');
    assert.deepEqual(plain(processReply.payload.value), value);
    callee.on('fail-app', async () => { throw new TypeError('integration application failure'); });
    const failed = await origin.callEvent('fail-app', { targetClientId: callee.clientId });
    assert.equal(failed.payload.value, null);
    assert.equal(failed.payload.error.type, 'TypeError');

    for (const kind of ['app', 'process']) {
        const incoming = event(recipient, 'app_result', envelope => envelope.payload?.value?.token === id);
        const accepted = kind === 'app' ?
            await origin.callEvent('echo-app', { targetClientId: callee.clientId, data: value, responseTo: recipient.clientId }) :
            await origin.process.call(`${callee.clientId}:echo`, value, { responseTo: recipient.clientId });
        assert.equal(accepted.type, 'ack');
        const envelope = await incoming;
        assert.equal(envelope.type, 'app_result');
        assert.equal(envelope.request_id, accepted.request_id);
        assert.deepEqual(plain(envelope.payload.value), value);
    }

    await callee.switchPool(config.pool, config.authToken ?? null);
    assert.equal(callee.endpoint.url, firstOwner.url, 'Same-pool ACK remains on the same owner');
    assert.equal((await origin.process.call(`${callee.clientId}:echo`, value)).payload.value.token, id);
    let handlerStarted;
    const started = new Promise(resolve => { handlerStarted = resolve; });
    const gate = new Promise(resolve => { releaseHandler = resolve; });
    callee.on('blocked-app', async () => { handlerStarted(); await gate; return 42; });
    const oldCall = origin.callEvent('blocked-app', { targetClientId: callee.clientId });
    const terminal = oldCall.then(reply => ({ reply }), error => ({ error }));
    await started;
    const switchAuth = config.switchAuthToken ?? config.authToken ?? null;
    await origin.switchPool(config.switchPool, switchAuth);
    assert.notEqual(origin.endpoint.url, firstOwner.url, 'The fixture pools must belong to different owners');
    assert.equal(origin.clientId, `${id}-origin`);
    assert.equal(origin.port, config.port);
    assert.equal(origin.wsPort, config.wsPort);
    const cancelled = await terminal;
    assert.ok(cancelled.error && !cancelled.reply, 'An old effect must not resolve from an obsolete owner');
    releaseHandler();
    releaseHandler = null;
    await Promise.all([callee.switchPool(config.switchPool, switchAuth), recipient.switchPool(config.switchPool, switchAuth)]);
    assert.equal(callee._processes.size, 0);
    assert.equal(callee.eventHandlers.size, 0);
    assert.equal(await origin.get(key, 'isolated'), 'isolated');
    assert.equal(Object.hasOwn(await origin.process.list(), `${callee.clientId}:echo`), false);
    await callee.process.register(data => data, 'echo'); // Explicit caller action, not SDK replay.
    assert.equal((await origin.process.call(`${callee.clientId}:echo`, value)).payload.value.token, id);

    await Promise.all(clients.map(client => client.switchPool(config.pool, config.authToken ?? null)));
    assert.deepEqual(plain(await recipient.get(key)), { ...value, changed: true });
    await origin.delete(key);
    origin.disconnect();
    assert.equal(origin.endpoint, null);
    const before = opened.length;
    await origin.connect();
    assert.equal(Number(new URL(opened[before]).port), config.wsPort, 'Explicit reconnect begins at the public entry');
    assert.equal(origin.endpoint.url, firstOwner.url);
    t.diagnostic(`Validated native WS public ${config.wsPort} -> owner ${firstOwner.wsPort}; cross-owner auth/switch and no replay`);
});
