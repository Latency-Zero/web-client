'use strict';

const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const { test } = require('node:test');
const vm = require('node:vm');

const source = readFileSync(join(__dirname, 'latzero-client.js'), 'utf8');
const checkpoint = () => new Promise(resolve => setImmediate(resolve));
const plain = value => JSON.parse(JSON.stringify(value));

function deferred() {
    let resolve, reject;
    const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
    return { promise, resolve, reject };
}

function observe(promise) {
    const state = { status: 'pending' };
    state.done = promise.then(value => {
        state.status = 'resolved';
        state.value = value;
    }, error => {
        state.status = 'rejected';
        state.error = error;
    });
    return state;
}

function sandbox(t, options = {}) {
    const clock = {
        now: 0, sequence: 0, timers: new Map(),
        setTimeout(fn, delay = 0) {
            const id = ++this.sequence;
            this.timers.set(id, { fn, at: this.now + Math.max(0, Number(delay)) });
            return id;
        },
        clearTimeout(id) { this.timers.delete(id); },
        async advance(ms) {
            const target = this.now + ms;
            // Run only explicit fake-clock deadlines, never wall-clock sleeps/poll loops.
            for (let budget = 0; budget < 10000; budget++) {
                const next = Array.from(this.timers).filter(([, timer]) => timer.at <= target)
                    .sort((a, b) => a[1].at - b[1].at || a[0] - b[0])[0];
                if (!next) {
                    this.now = target;
                    await checkpoint();
                    return;
                }
                this.now = next[1].at;
                this.timers.delete(next[0]);
                next[1].fn();
                await checkpoint();
            }
            throw new Error('Fake timer budget exhausted');
        }
    };
    const sockets = [];
    class MockWebSocket {
        static CONNECTING = 0;
        static OPEN = 1;
        static CLOSING = 2;
        static CLOSED = 3;
        constructor(url) {
            this.url = url;
            this.readyState = MockWebSocket.CONNECTING;
            this.bufferedAmount = 0;
            this.sent = [];
            this.onSend = null;
            sockets.push(this);
        }
        open() {
            this.readyState = MockWebSocket.OPEN;
            this.onopen?.();
        }
        send(text) {
            if (this.readyState !== MockWebSocket.OPEN) throw new Error('Socket is not open');
            if (this.sendFailure) throw this.sendFailure;
            const message = JSON.parse(text);
            this.sent.push(message);
            this.onSend?.(message);
        }
        receive(message) {
            this.onmessage?.({ data: typeof message === 'string' ? message : JSON.stringify(message) });
        }
        close() {
            if (this.readyState === MockWebSocket.CLOSED) return;
            this.readyState = MockWebSocket.CLOSED;
            this.onclose?.();
        }
        fail() { this.onerror?.({ message: 'mock connection failure' }); }
    }
    class TestCustomEvent extends Event {
        constructor(type, options = {}) { super(type); this.detail = options.detail; }
    }
    class TestDate extends Date {
        static now() { return 1700000000000 + clock.now; }
    }
    const math = Object.create(Math);
    math.random = () => 0.5;
    const context = vm.createContext({
        window: {}, WebSocket: MockWebSocket, URL, EventTarget, CustomEvent: TestCustomEvent,
        TextEncoder, Date: TestDate, Math: math, performance: { now: () => clock.now },
        setTimeout: clock.setTimeout.bind(clock), clearTimeout: clock.clearTimeout.bind(clock),
        console
    });
    vm.runInContext(source, context, { filename: 'latzero-client.js' });
    const Client = context.window.LatZeroWebClient;
    const clients = [];
    const create = (extra, pool = 'old-pool') => {
        const client = new Client('latzero://browser', pool, {
            autoConnect: false, timeout: 100, maxReconnectAttempts: 0, ...options, ...extra
        });
        clients.push(client);
        return client;
    };
    t.after(async () => {
        clients.forEach(client => client.disconnect());
        await checkpoint();
        for (const client of clients) {
            assert.equal(client.pending.size, 0, 'No pending waiter leaks');
            assert.equal(client._outbox.length, 0, 'No queued frame leaks');
            assert.equal(client._queuedBytes, 0, 'No queued byte leaks');
            assert.equal(client._activeHandlers.size, 0, 'Every test releases its mock handlers');
            assert.equal(client._deferredNotifications.length, 0, 'No pool-switch notification leaks');
            assert.equal(client._deferredNotificationBytes, 0, 'No deferred notification byte leaks');
        }
        assert.equal(clock.timers.size, 0, 'No timeout/reconnect/send-poll timer leaks');
    });
    return { clock, sockets, Client, create, MockWebSocket };
}

function ack(ws, request, payload = {}) {
    ws.receive({ type: 'ack', request_id: request.request_id, pool: request.pool, payload });
}

function result(ws, request, value, error = null) {
    ws.receive({
        type: 'app_result', request_id: request.request_id, client_id: 'callee', pool: request.pool,
        payload: { request_id: request.request_id, event: 'compute', value, error }
    });
}

function invocation(ws, id, event = 'compute', data = {}, type = 'call_app', pool = 'old-pool') {
    ws.receive({
        type, request_id: id, client_id: 'caller', pool,
        payload: type === 'call_app' ? { event, data } : { process_id: event, data }
    });
}

function redirectMessage(request, payload = {}, envelope = {}) {
    const pool = request.payload.pool;
    return {
        type: 'redirect', request_id: request.request_id, client_id: request.client_id, pool,
        payload: {
            protocol: 'pool_redirect_v1', host: '127.0.0.1', port: 21130, ws_port: 22130,
            pool, pod_index: 0, pod_count: 2, router_host: '127.0.0.1', router_port: 14130,
            router_ws_port: 14131, cluster_id: 'mock-cluster', ...payload
        }, ...envelope
    };
}

async function joinRequest(ws) {
    ws.open();
    assert.equal(ws.sent[0].type, 'hello');
    assert.equal(ws.sent[0].pool, null);
    ack(ws, ws.sent[0]);
    await checkpoint();
    const request = ws.sent.at(-1);
    assert.equal(request.type, 'join_pool');
    return request;
}

async function connect(h, client = h.create()) {
    const promise = client.connect();
    const ws = h.sockets.at(-1);
    ws.open();
    assert.equal(ws.sent[0].type, 'hello');
    assert.equal(ws.sent[0].pool, null);
    ack(ws, ws.sent[0], { server: 'latzero-server' });
    await checkpoint();
    assert.equal(ws.sent[1].type, 'join_pool');
    assert.equal(ws.sent[1].payload.client_id, client.clientId);
    ack(ws, ws.sent[1], { joined: true });
    await promise;
    ws.sent.length = 0;
    assert.equal(h.clock.timers.size, 0);
    return { client, ws };
}

const regression = (name, fn) => test(name, { timeout: 3000 }, fn);

regression('one connection promise stays unready until join; join ACK commits before next frame', async t => {
    const h = sandbox(t);
    const client = h.create();
    client.on('compute', () => 7);
    const ready = client.connect();
    assert.equal(client.connect(), ready);
    const ws = h.sockets[0];
    assert.equal(client.connected, false);
    ws.open();
    assert.equal(client.connected, false);
    ack(ws, ws.sent[0]);
    await checkpoint();
    assert.equal(client.connected, false);
    ack(ws, ws.sent[1]);
    invocation(ws, 'opaque-after-join');
    await ready;
    await checkpoint();
    assert.equal(client.connected, true);
    assert.equal(ws.sent.at(-1).request_id, 'opaque-after-join');
    assert.equal(ws.sent.at(-1).payload.value, 7);
    assert.equal(h.clock.timers.size, 0);
});

regression('shared request IDs do not swallow presence, buffer or self event pushes', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    const seen = [];
    client.addEventListener('presence', event => seen.push(event.detail));
    client.addEventListener('bufferUpdate', event => seen.push(event.detail));
    client.on('notice', data => seen.push(data));
    const waiting = client.emitEvent('notice', { targetClientId: 'browser', data: { n: 1 } });
    const request = ws.sent[0];
    ws.receive({ type: 'presence_update', request_id: request.request_id, payload: { n: 2 } });
    ws.receive({ type: 'buffer_update', request_id: request.request_id, payload: { n: 3 } });
    ws.receive({ ...request, type: 'emit_event' });
    await checkpoint();
    assert.equal(client.pending.size, 1);
    assert.equal(seen.length, 3);
    ack(ws, request, { delivered: true });
    await waiting;
    assert.equal(h.clock.timers.size, 0);
});

for (const method of ['process', 'event']) {
    for (const order of ['ack-first', 'result-first']) {
        regression(`${method} RPC waits for terminal app_result (${order})`, async t => {
            const h = sandbox(t);
            const { client, ws } = await connect(h);
            const state = observe(method === 'process' ? client.process.call('callee:compute') :
                client.callEvent('compute', { targetClientId: 'callee' }));
            const request = ws.sent[0];
            assert.equal(request.payload.timeout, 0.1);
            if (order === 'ack-first') {
                ack(ws, request, { queued: true });
                await checkpoint();
                assert.equal(state.status, 'pending');
                assert.equal(client.pending.size, 1);
                assert.equal(h.clock.timers.size, 1);
            }
            result(ws, request, 42);
            await state.done;
            if (order === 'result-first') ack(ws, request, { queued: true });
            assert.equal(state.status, 'resolved');
            assert.equal(state.value.type, 'app_result');
            assert.equal(state.value.request_id, request.request_id);
            assert.deepEqual(plain(state.value.payload), {
                request_id: request.request_id, event: 'compute', value: 42, error: null
            });
            assert.equal(h.clock.timers.size, 0);
        });
    }
}

regression('self invocation with matching origin ID runs while terminal waiter remains', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    const registration = client.process.register(data => data.a + data.b, 'sum');
    ack(ws, ws.sent[0]);
    await registration;
    ws.sent.length = 0;
    const state = observe(client.process.call('browser:sum', { a: 2, b: 5 }));
    const request = ws.sent[0];
    invocation(ws, request.request_id, 'browser:sum', { a: 2, b: 5 });
    await checkpoint();
    assert.equal(ws.sent[1].type, 'app_result');
    assert.equal(ws.sent[1].payload.value, 7);
    ack(ws, ws.sent[1], { delivered: true });
    ack(ws, request, { queued: true });
    assert.equal(state.status, 'pending');
    result(ws, request, 7);
    await state.done;
    assert.equal(state.value.payload.value, 7);
});

regression('opaque callee IDs are echoed without requiring new payload echo fields', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    const gate = deferred();
    client.on('compute', async data => (await gate.promise) + data.n);
    invocation(ws, 'hop-opaque', 'compute', { n: 2 });
    await checkpoint();
    assert.equal(ws.sent.length, 0);
    gate.resolve(40);
    await checkpoint();
    assert.deepEqual(ws.sent[0], {
        type: 'app_result', request_id: 'hop-opaque', client_id: 'browser', pool: 'old-pool',
        payload: { value: 42, error: null }
    });
});

regression('mock broker routes opaque self and third-party calls end to end', async t => {
    const h = sandbox(t);
    const origin = h.create();
    const recipient = new h.Client('latzero://recipient', 'old-pool', {
        autoConnect: false, timeout: 100, maxReconnectAttempts: 0
    });
    t.after(() => recipient.disconnect());
    const caller = await connect(h, origin);
    const receiver = await connect(h, recipient);
    const routes = new Map();
    const peers = new Map([['browser', caller.ws], ['recipient', receiver.ws]]);
    let hops = 0;
    caller.ws.onSend = message => {
        if (message.type === 'register_process') ack(caller.ws, message);
        if (message.type === 'call_process') {
            const hop = `opaque-hop-${++hops}`;
            routes.set(hop, { request: message, destination: message.payload.response_to || 'browser' });
            invocation(caller.ws, hop, message.payload.process_id, message.payload.data);
            ack(caller.ws, message, { queued: true, request_id: message.request_id });
        }
        if (message.type === 'app_result') {
            const route = routes.get(message.request_id);
            assert.ok(route, 'Reply echoes only the opaque hop ID');
            routes.delete(message.request_id);
            ack(caller.ws, message, { delivered: true });
            peers.get(route.destination).receive({
                type: 'app_result', request_id: route.request.request_id, client_id: 'browser', pool: 'old-pool',
                payload: { request_id: route.request.request_id, ...message.payload, response_to: route.destination }
            });
        }
    };
    await origin.process.register(async ({ a, b }) => a + b, 'sum');
    const direct = await origin.process.call('browser:sum', { a: 20, b: 22 });
    assert.equal(direct.type, 'app_result');
    assert.equal(direct.payload.value, 42);
    assert.ok(direct.request_id.startsWith('req_'));

    const hook = deferred();
    recipient.addEventListener('app_result', event => hook.resolve(event.detail));
    const accepted = await origin.process.call('browser:sum', { a: 3, b: 4 }, { responseTo: 'recipient' });
    assert.equal(accepted.type, 'ack');
    await checkpoint();
    const delivered = await hook.promise;
    assert.equal(delivered.payload.value, 7);
    assert.equal(delivered.request_id, accepted.request_id);
    assert.equal(delivered.payload.request_id, accepted.request_id);
    assert.equal(routes.size, 0);
    assert.equal(h.clock.timers.size, 0);
});

regression('concurrent callEvent promises correlate by their request IDs without polling', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    const first = client.callEvent('compute', { targetClientId: 'callee' });
    const second = client.callEvent('compute', { targetClientId: 'callee' });
    assert.notEqual(ws.sent[0].request_id, ws.sent[1].request_id);
    result(ws, ws.sent[1], 2);
    result(ws, ws.sent[0], 1);
    assert.equal((await first).payload.value, 1);
    assert.equal((await second).payload.value, 2);
    assert.equal(h.clock.timers.size, 0);
});

for (const method of ['process', 'event']) {
    regression(`${method} responseTo self still waits; third party returns ACK metadata and full result hook`, async t => {
        const h = sandbox(t);
        const { client, ws } = await connect(h);
        const call = responseTo => method === 'process' ?
            client.process.call('callee:compute', {}, { responseTo }) :
            client.callEvent('compute', { targetClientId: 'callee', responseTo });
        const self = observe(call('browser'));
        ack(ws, ws.sent[0], { queued: true });
        await checkpoint();
        assert.equal(self.status, 'pending');
        result(ws, ws.sent[0], 1);
        await self.done;

        const hooks = [];
        client.addEventListener('app_result', event => hooks.push(event.detail));
        const third = observe(call('recipient'));
        const request = ws.sent.at(-1);
        ack(ws, request, { delivered: true });
        result(ws, request, 99);
        await checkpoint();
        assert.equal(third.status, 'pending');
        assert.equal(hooks.length, 1);
        assert.equal(hooks[0].request_id, request.request_id);
        assert.equal(hooks[0].pool, 'old-pool');
        ack(ws, request, { queued: true, request_id: request.request_id, response_to: 'recipient' });
        await third.done;
        assert.equal(third.value.type, 'ack');
        assert.equal(third.value.payload.queued, true);
        assert.equal(third.value.request_id, request.request_id);
        assert.equal(h.clock.timers.size, 0);
    });
}

regression('app_result never settles an ordinary ACK waiter; application errors are returned, not thrown', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    const ordinary = observe(client.get('key'));
    result(ws, ws.sent[0], 'unsolicited');
    await checkpoint();
    assert.equal(ordinary.status, 'pending');
    ack(ws, ws.sent[0], { exists: true, entry: { value: false } });
    await ordinary.done;
    assert.equal(ordinary.value, false);
    const call = client.process.call('callee:compute');
    result(ws, ws.sent.at(-1), null, { type: 'ValueError', message: 'bad application input' });
    assert.deepEqual(plain((await call).payload.error), { type: 'ValueError', message: 'bad application input' });
});

for (const [name, handler, expected] of [
    ['rejected async', async () => { throw new TypeError('handler rejected'); }, 'handler rejected'],
    ['circular result', () => { const value = {}; value.self = value; return value; }, 'circular'],
    ['BigInt result', () => 1n, 'BigInt'],
    ['function result', () => () => 1, 'not JSON-serializable'],
    ['symbol result', () => Symbol('result'), 'not JSON-serializable'],
    ['toJSON omits root', () => ({ toJSON() { return undefined; } }), 'not JSON-serializable'],
    ['throwing toJSON', () => ({ toJSON() { throw new Error('toJSON failed'); } }), 'toJSON failed'],
    ['thrown string', () => { throw 'failure string'; }, 'failure string'],
    ['hostile thrown proxy', () => { throw new Proxy({}, { get() { throw new Error('poison'); } }); }, 'Application handler failed']
]) {
    regression(`${name} produces a safe value/error application reply`, async t => {
        const h = sandbox(t);
        const { client, ws } = await connect(h);
        client.on('compute', handler);
        invocation(ws, 'opaque-error');
        await checkpoint();
        assert.equal(ws.sent.length, 1);
        assert.equal(ws.sent[0].payload.value, null);
        assert.match(ws.sent[0].payload.error.message, new RegExp(expected, 'i'));
        assert.equal(client.connected, true);
    });
}

regression('undefined and legacy call_process handler results retain value/error reply shape', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    client.on('browser:compute', async () => undefined);
    invocation(ws, 'legacy-hop', 'browser:compute', {}, 'call_process');
    await checkpoint();
    assert.deepEqual(ws.sent[0].payload, { value: null, error: null });
});

regression('registration replaces before advertising and rolls back a rejected replacement', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    const first = client.process.register(() => 'old', 'compute');
    ack(ws, ws.sent[0]);
    await first;
    ws.sent.length = 0;
    ws.onSend = message => {
        if (message.type === 'register_process') invocation(ws, 'during-register', 'browser:compute');
    };
    const replacement = observe(client.process.register(() => 'new', 'compute'));
    const request = ws.sent[0];
    await checkpoint();
    assert.equal(ws.sent[1].payload.value, 'new');
    ws.receive({ type: 'error', request_id: request.request_id, payload: { code: 'denied', message: 'rejected' } });
    await replacement.done;
    assert.equal(replacement.error.code, 'denied');
    invocation(ws, 'after-rollback', 'browser:compute');
    await checkpoint();
    assert.equal(ws.sent.at(-1).payload.value, 'old');
    ws.onSend = null;
});

regression('failed initial registration and unregister rollback do not leave stale handlers', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    const first = observe(client.process.register(() => 1, 'compute'));
    ws.receive({ type: 'error', request_id: ws.sent[0].request_id, payload: { code: 'denied' } });
    await first.done;
    assert.equal(client._processes.size, 0);
    const registration = client.process.register(() => 2, 'compute');
    ack(ws, ws.sent.at(-1));
    await registration;
    const removed = observe(client.process.unregister('compute'));
    const removeRequest = ws.sent.at(-1);
    invocation(ws, 'during-unregister', 'browser:compute');
    await checkpoint();
    assert.equal(ws.sent.at(-1).payload.error.type, 'NoHandler');
    ws.receive({ type: 'error', request_id: removeRequest.request_id, payload: { code: 'denied' } });
    await removed.done;
    invocation(ws, 'after-unregister-rollback', 'browser:compute');
    await checkpoint();
    assert.equal(ws.sent.at(-1).payload.value, 2);
});

regression('registration timeout rolls back the replacement without automatic retry', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    const registered = client.process.register(() => 'old', 'compute');
    ack(ws, ws.sent[0]);
    await registered;
    const replacement = observe(client.process.register(() => 'new', 'compute'));
    const sent = ws.sent.length;
    await h.clock.advance(100);
    await replacement.done;
    assert.equal(replacement.error.code, 'timeout');
    assert.equal(ws.sent.length, sent);
    invocation(ws, 'after-registration-timeout', 'browser:compute');
    await checkpoint();
    assert.equal(ws.sent.at(-1).payload.value, 'old');
});

regression('replacement does not change an already admitted async handler or its reply correlation', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    const gate = deferred();
    const first = client.process.register(() => gate.promise, 'compute');
    ack(ws, ws.sent[0]);
    await first;
    ws.sent.length = 0;
    invocation(ws, 'old-admitted-hop', 'browser:compute');
    await checkpoint();
    const replacement = client.process.register(() => 'new', 'compute');
    ack(ws, ws.sent[0]);
    await replacement;
    invocation(ws, 'new-admitted-hop', 'browser:compute');
    await checkpoint();
    gate.resolve('old');
    await checkpoint();
    assert.equal(ws.sent.find(message => message.request_id === 'old-admitted-hop').payload.value, 'old');
    assert.equal(ws.sent.find(message => message.request_id === 'new-admitted-hop').payload.value, 'new');
});

regression('broadcast waits for its ACK while independent child results reach the full-envelope hook', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    const children = [];
    client.addEventListener('app_result', event => children.push(event.detail));
    const state = observe(client.process.broadcast('compute', {}, { timeout: 250 }));
    const request = ws.sent[0];
    assert.equal(request.payload.timeout, 0.25);
    for (const id of ['opaque-child-one', 'opaque-child-two']) {
        ws.receive({
            type: 'app_result', request_id: id, pool: 'old-pool',
            payload: { request_id: id, parent_request_id: request.request_id, value: id, error: null }
        });
    }
    await checkpoint();
    assert.equal(state.status, 'pending');
    assert.equal(children.length, 2);
    assert.notEqual(children[0].request_id, children[1].request_id);
    ack(ws, request, { invoked_processes: ['one:compute', 'two:compute'] });
    await state.done;
    assert.deepEqual(plain(state.value), ['one:compute', 'two:compute']);
    assert.equal(h.clock.timers.size, 0);
});

regression('pool switch drains admitted handlers, retains old reply context and commits before next push', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    const gate = deferred();
    const registration = client.process.register(() => gate.promise, 'compute');
    ack(ws, ws.sent[0]);
    await registration;
    ws.sent.length = 0;
    invocation(ws, 'old-hop', 'browser:compute');
    await checkpoint();
    const switched = client.switchPool('new-pool');
    const blocked = observe(client.get('blocked'));
    await blocked.done;
    assert.equal(blocked.error.code, 'pool_switch_in_progress');
    assert.equal(ws.sent.length, 0);
    gate.resolve('old-result');
    await checkpoint();
    assert.equal(ws.sent[0].type, 'app_result');
    assert.equal(ws.sent[0].pool, 'old-pool');
    assert.equal(ws.sent[1].type, 'switch_pool');
    ack(ws, ws.sent[1], { pool: 'new-pool' });
    invocation(ws, 'new-hop', 'browser:compute', {}, 'call_app', 'new-pool');
    await switched;
    await checkpoint();
    assert.equal(client.poolName, 'new-pool');
    assert.equal(client._processes.size, 0);
    assert.equal(ws.sent.at(-1).pool, 'new-pool');
    assert.equal(ws.sent.at(-1).payload.error.type, 'NoHandler');
    assert.equal(h.clock.timers.size, 0);
});

regression('switch failure restores admission; drain time and ACK share one deadline', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    const gate = deferred();
    client.on('compute', () => gate.promise);
    invocation(ws, 'old-hop');
    await checkpoint();
    const switchState = observe(client.switchPool('new-pool'));
    await h.clock.advance(60);
    gate.resolve(1);
    await checkpoint();
    const request = ws.sent.find(message => message.type === 'switch_pool');
    assert.ok(request);
    await h.clock.advance(39);
    assert.equal(switchState.status, 'pending');
    await h.clock.advance(1);
    await switchState.done;
    assert.equal(switchState.error.code, 'timeout');
    assert.equal(client.connected, false, 'Timed-out transmitted switch cannot retain ambiguous membership');
});

regression('quiescence timeout sends no switch and preserves the old connection/handler', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    const gate = deferred();
    client.on('compute', () => gate.promise);
    invocation(ws, 'old-hop');
    await checkpoint();
    const state = observe(client.switchPool('new-pool'));
    await h.clock.advance(100);
    await state.done;
    assert.equal(state.error.code, 'timeout');
    assert.equal(client.connected, true);
    assert.equal(client.poolName, 'old-pool');
    assert.equal(ws.sent.length, 0);
    gate.resolve(3);
    await checkpoint();
    assert.equal(ws.sent[0].payload.value, 3);
});

regression('explicit switch error preserves old registrations and does not close the connection', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    client.on('compute', () => 9);
    const state = observe(client.switchPool('denied-pool'));
    await checkpoint();
    ws.receive({ type: 'error', request_id: ws.sent[0].request_id, payload: { code: 'auth_failed' } });
    await state.done;
    assert.equal(client.connected, true);
    assert.equal(client.poolName, 'old-pool');
    invocation(ws, 'after-failed-switch');
    await checkpoint();
    assert.equal(ws.sent.at(-1).payload.value, 9);
});

regression('same-pool switch is idempotent and keeps registrations and pending terminal work', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    const registered = client.process.register(() => 42, 'compute');
    ack(ws, ws.sent[0]);
    await registered;
    const pending = observe(client.process.call('callee:compute'));
    const call = ws.sent.at(-1);
    const switched = client.switchPool('old-pool');
    await checkpoint();
    ack(ws, ws.sent.at(-1));
    await switched;
    assert.equal(client._processes.size, 1);
    assert.equal(pending.status, 'pending');
    invocation(ws, 'after-idempotent-rejoin', 'browser:compute');
    await checkpoint();
    assert.equal(ws.sent.at(-1).payload.value, 42);
    result(ws, call, 7);
    await pending.done;
    assert.equal(pending.value.payload.value, 7);
});

regression('server old-pool presence during switch is deferred, not mistaken for overload or new-pool state', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    const seen = [];
    client.addEventListener('presence', event => seen.push(event.detail));
    const rejected = observe(client.switchPool('denied-pool'));
    await checkpoint();
    ws.receive({ type: 'presence_update', pool: 'old-pool', payload: { action: 'left' } });
    assert.equal(client.connected, true);
    assert.equal(seen.length, 0);
    ws.receive({ type: 'error', request_id: ws.sent[0].request_id, payload: { code: 'denied' } });
    await rejected.done;
    assert.equal(seen.length, 1, 'Failed switch preserves and releases old-pool notifications');

    const accepted = client.switchPool('new-pool');
    await checkpoint();
    ws.receive({ type: 'presence_update', pool: 'old-pool', payload: { action: 'left-again' } });
    ack(ws, ws.sent.at(-1));
    await accepted;
    assert.equal(client.connected, true);
    assert.equal(client.poolName, 'new-pool');
    assert.equal(seen.length, 1, 'Successful switch fences the old-pool notification');
});

regression('pool-switch deferred notifications have finite count and byte admission', async t => {
    const h = sandbox(t, { maxQueuedMessages: 1 });
    const { client, ws } = await connect(h);
    const state = observe(client.switchPool('new-pool'));
    await checkpoint();
    ws.receive({ type: 'presence_update', pool: 'old-pool', payload: { n: 1 } });
    assert.equal(client._deferredNotifications.length, 1);
    ws.receive({ type: 'presence_update', pool: 'old-pool', payload: { n: 2 } });
    await state.done;
    assert.equal(client.connected, false);
    assert.equal(client._deferredNotifications.length, 0);
    assert.equal(client._deferredNotificationBytes, 0);
});

regression('old-pool invocations after switch transmission are fenced until acceptance/rejection', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    let calls = 0;
    client.on('compute', () => { calls++; return 9; });
    const accepted = client.switchPool('new-pool');
    await checkpoint();
    const request = ws.sent[0];
    invocation(ws, 'old-after-switch');
    await checkpoint();
    assert.equal(calls, 0);
    assert.equal(ws.sent.length, 1, 'No old-pool reply sent after a transmitted switch');
    assert.equal(client._outbox.length, 1, 'Safe rejection remains bounded while membership is ambiguous');
    ack(ws, request);
    await accepted;
    assert.equal(client._outbox.length, 0);
    assert.equal(h.clock.timers.size, 0);

    client.on('compute', () => { calls++; return 8; });
    ws.sent.length = 0;
    const rejected = observe(client.switchPool('denied-pool'));
    await checkpoint();
    invocation(ws, 'same-pool-after-denied', 'compute', {}, 'call_app', 'new-pool');
    await checkpoint();
    assert.equal(ws.sent.length, 1);
    ws.receive({ type: 'error', request_id: ws.sent[0].request_id, payload: { code: 'denied' } });
    await rejected.done;
    assert.equal(ws.sent[1].pool, 'new-pool');
    assert.equal(ws.sent[1].payload.error.type, 'PoolSwitching');
    assert.equal(client.connected, true);
});

regression('async replies from a disconnected socket never reach a reconnected socket', async t => {
    const h = sandbox(t);
    const { client, ws: old } = await connect(h);
    const gate = deferred();
    client.on('compute', () => gate.promise);
    invocation(old, 'old-hop');
    await checkpoint();
    client.disconnect();
    const { ws: current } = await connect(h, client);
    gate.resolve(42);
    await checkpoint();
    assert.equal(old.sent.length, 0);
    assert.equal(current.sent.length, 0);
    old.receive({ type: 'presence_update', payload: {} });
    assert.equal(client.connected, true);
});

regression('wrong-pool replies and invocations on the same socket cannot consume current work', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    let calls = 0;
    client.on('compute', () => { calls++; return 42; });
    const state = observe(client.process.call('callee:compute'));
    const request = ws.sent[0];
    invocation(ws, request.request_id, 'compute', {}, 'call_app', 'another-pool');
    result(ws, { ...request, pool: 'another-pool' }, 99);
    await checkpoint();
    assert.equal(calls, 0);
    assert.equal(state.status, 'pending');
    result(ws, request, 42);
    await state.done;
    assert.equal(state.value.payload.value, 42);
});

regression('async notification failure is isolated while handler execution remains ordered', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    const seen = [], errors = [];
    client.addEventListener('error', event => errors.push(event.detail));
    client.on('notice', async () => { seen.push(1); throw new Error('notification failed'); });
    client.on('notice', async () => { seen.push(2); });
    ws.receive({ type: 'emit_event', payload: { event: 'notice', data: {} } });
    await checkpoint();
    assert.deepEqual(seen, [1, 2]);
    assert.equal(errors[0].message, 'notification failed');
    assert.equal(client.connected, true);
    assert.equal(client._activeHandlers.size, 0);
});

regression('TTL and API timeout milliseconds become fractional wire seconds', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    for (const [milliseconds, seconds] of [[30000, 30], [125, 0.125], [0, 0], [null, null]]) {
        const set = client.set('temp', 0, { autoClean: milliseconds });
        assert.equal(ws.sent.at(-1).payload.ttl, seconds);
        ack(ws, ws.sent.at(-1));
        await set;
    }
    for (const invalid of [-1, Infinity, NaN, true]) {
        await assert.rejects(client.set('temp', 1, { autoClean: invalid }), /autoClean/);
    }
    const call = client.callEvent('compute', { targetClientId: 'callee', timeout: 250 });
    assert.equal(ws.sent.at(-1).payload.timeout, 0.25);
    result(ws, ws.sent.at(-1), 1);
    await call;
});

regression('one call deadline includes queued send, ACK and terminal result', async t => {
    const h = sandbox(t, { maxBufferedAmount: 1000 });
    const { client, ws } = await connect(h);
    ws.bufferedAmount = 1000;
    const state = observe(client.process.call('callee:compute', {}, { timeout: 100 }));
    assert.equal(ws.sent.length, 0);
    await h.clock.advance(40);
    ws.bufferedAmount = 0;
    await h.clock.advance(10);
    assert.equal(ws.sent.length, 1);
    assert.equal(ws.sent[0].payload.timeout, 0.05);
    ack(ws, ws.sent[0], { queued: true });
    await h.clock.advance(49);
    assert.equal(state.status, 'pending');
    await h.clock.advance(1);
    await state.done;
    assert.equal(state.error.code, 'timeout');
    assert.equal(state.error.uncertain, true);
    assert.equal(h.clock.timers.size, 0);
    assert.equal(ws.sent.length, 1, 'No retry after transmitted timeout');
});

regression('queued timeout cancels before transmission and can never execute after drain', async t => {
    const h = sandbox(t, { maxBufferedAmount: 1000 });
    const { client, ws } = await connect(h);
    ws.bufferedAmount = 1000;
    const state = observe(client.process.call('callee:compute'));
    await h.clock.advance(100);
    await state.done;
    assert.equal(state.error.uncertain, false);
    assert.equal(client._outbox.length, 0);
    ws.bufferedAmount = 0;
    await h.clock.advance(100);
    assert.equal(ws.sent.length, 0);
    assert.equal(h.clock.timers.size, 0);
});

regression('late reply cannot beat the monotonic deadline even when its timer has not run', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    const late = [];
    client.addEventListener('app_result', event => late.push(event.detail));
    const state = observe(client.process.call('callee:compute'));
    const request = ws.sent[0];
    h.clock.now = 100; // Model a delayed event loop without executing the scheduled timer first.
    result(ws, request, 42);
    await state.done;
    assert.equal(state.status, 'rejected');
    assert.equal(state.error.code, 'timeout');
    assert.equal(late.length, 1);
    assert.equal(h.clock.timers.size, 0);
});

regression('pending, queued message and queued byte limits reject before acceptance', async t => {
    const h = sandbox(t, { maxPendingRequests: 2, maxQueuedMessages: 1, maxBufferedAmount: 1000 });
    const { client, ws } = await connect(h);
    ws.bufferedAmount = 1000;
    const first = observe(client.get('one'));
    const excessQueue = observe(client.get('two'));
    await excessQueue.done;
    assert.equal(excessQueue.error.code, 'overloaded');
    assert.equal(excessQueue.error.uncertain, false);
    assert.equal(client.pending.size, 1);
    assert.equal(client._outbox.length, 1);
    ws.bufferedAmount = 0;
    await h.clock.advance(10);
    const second = observe(client.get('two'));
    const excessPending = observe(client.get('three'));
    await excessPending.done;
    assert.equal(excessPending.error.code, 'overloaded');
    assert.equal(client.pending.size, 2);
    ack(ws, ws.sent[0], { exists: false });
    ack(ws, ws.sent[1], { exists: false });
    await Promise.all([first.done, second.done]);

    client.maxQueuedBytes = 1;
    const excessBytes = observe(client.get('bytes'));
    await excessBytes.done;
    assert.equal(excessBytes.error.code, 'overloaded');
    assert.equal(client._queuedBytes, 0);
});

regression('batches are bounded and retain their slot until all admitted items settle', async t => {
    const h = sandbox(t, { maxBatchSize: 2, maxPendingBatches: 1 });
    const { client, ws } = await connect(h);
    await assert.rejects(client.mget(['a', 'b', 'c']), /Batch item/);
    assert.equal(ws.sent.length, 0);
    const batch = observe(client.mget(['a', 'b']));
    await checkpoint();
    ws.receive({ type: 'error', request_id: ws.sent[0].request_id, payload: { code: 'denied' } });
    await checkpoint();
    assert.equal(batch.status, 'pending');
    const excess = observe(client.deleteMany(['c']));
    await excess.done;
    assert.equal(excess.error.code, 'overloaded');
    ack(ws, ws.sent[1], { exists: false });
    await batch.done;
    assert.equal(batch.error.code, 'denied');
    assert.equal(client._activeBatches, 0);
});

regression('values/items apply batch admission to listed keys before issuing reads', async t => {
    const h = sandbox(t, { maxBatchSize: 1 });
    const { client, ws } = await connect(h);
    for (const method of ['values', 'items']) {
        const state = observe(client[method]());
        ack(ws, ws.sent.at(-1), { keys: ['a', 'b'] });
        await state.done;
        assert.equal(state.error.code, 'overloaded');
        assert.equal(client._activeBatches, 0);
    }
    assert.equal(ws.sent.length, 2);
});

regression('handler admission rejects excess RPC with a safe reply using bounded control reserve', async t => {
    const h = sandbox(t, { maxActiveHandlers: 1, maxPendingRequests: 1, maxQueuedMessages: 1, maxBufferedAmount: 1000 });
    const { client, ws } = await connect(h);
    const gate = deferred();
    client.on('compute', () => gate.promise);
    invocation(ws, 'active-hop');
    await checkpoint();
    ws.bufferedAmount = 1000;
    const queued = observe(client.get('queued'));
    invocation(ws, 'excess-hop');
    await checkpoint();
    assert.equal(client._activeHandlers.size, 1);
    assert.equal(client._outbox.length, 2);
    ws.bufferedAmount = 0;
    await h.clock.advance(10);
    ack(ws, ws.sent[0], { exists: false });
    await queued.done;
    assert.equal(ws.sent[1].payload.error.type, 'Overloaded');
    gate.resolve(42);
    await checkpoint();
    assert.equal(ws.sent.at(-1).payload.value, 42);
});

regression('notification overload fails the connection rather than silently dropping subscribed work', async t => {
    const h = sandbox(t, { maxActiveHandlers: 1 });
    const { client, ws } = await connect(h);
    const gate = deferred();
    client.on('notice', () => gate.promise);
    ws.receive({ type: 'emit_event', payload: { event: 'notice', data: {} } });
    await checkpoint();
    ws.receive({ type: 'emit_event', payload: { event: 'notice', data: {} } });
    assert.equal(client.connected, false);
    gate.resolve();
    await checkpoint();
});

regression('buffer notification admission overload also fails explicitly rather than bypassing handler limits', async t => {
    const h = sandbox(t, { maxActiveHandlers: 1 });
    const { client, ws } = await connect(h);
    const gate = deferred();
    client.on('compute', () => gate.promise);
    invocation(ws, 'active-hop');
    await checkpoint();
    const seen = [];
    client.addEventListener('bufferUpdate', event => seen.push(event.detail));
    ws.receive({ type: 'buffer_update', payload: { key: 'one', value: 1 } });
    assert.equal(client.connected, false);
    assert.equal(seen.length, 0);
    gate.resolve();
    await checkpoint();
});

regression('encoding/send failure and non-OPEN socket state clear every waiter and timer', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    const circular = {}; circular.self = circular;
    await assert.rejects(client.sendRequest('set_buffer', { value: circular }), /circular/i);
    assert.equal(client.pending.size, 0);
    assert.equal(h.clock.timers.size, 0);
    const first = observe(client.get('one'));
    ws.sendFailure = new Error('send failed');
    const second = observe(client.get('two'));
    await Promise.all([first.done, second.done]);
    assert.equal(client.connected, false);
    assert.equal(client.pending.size, 0);
    assert.equal(h.clock.timers.size, 0);
    await assert.rejects(client.get('closed'), /Not connected/);
});

regression('oversized handler output becomes a small safe application error', async t => {
    const h = sandbox(t, { maxFrameBytes: 700, maxBufferedAmount: 1000 });
    const { client, ws } = await connect(h);
    client.on('compute', () => 'x'.repeat(1000));
    invocation(ws, 'large-result');
    await checkpoint();
    assert.equal(ws.sent[0].payload.value, null);
    assert.match(ws.sent[0].payload.error.message, /budget/);
    assert.equal(client.connected, true);
});

regression('handler result custom serialization is evaluated only once', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    let serializations = 0;
    client.on('compute', () => ({ toJSON() { return ++serializations; } }));
    invocation(ws, 'once-hop');
    await checkpoint();
    assert.equal(serializations, 1);
    assert.equal(ws.sent[0].payload.value, 1);
});

regression('queued handler reply timeout fails explicitly instead of retaining or dropping it silently', async t => {
    const h = sandbox(t, { maxBufferedAmount: 1000 });
    const { client, ws } = await connect(h);
    ws.bufferedAmount = 1000;
    client.on('compute', () => 42);
    invocation(ws, 'blocked-reply');
    await checkpoint();
    assert.equal(client._outbox.length, 1);
    await h.clock.advance(100);
    assert.equal(client.connected, false);
    assert.equal(client._outbox.length, 0);
    assert.equal(h.clock.timers.size, 0);
    assert.equal(ws.sent.length, 0);
});

regression('oversized and binary incoming frames fail explicitly before dispatch', async t => {
    const h = sandbox(t, { maxFrameBytes: 700 });
    const first = await connect(h);
    first.ws.receive('x'.repeat(701));
    assert.equal(first.client.connected, false);
    const second = await connect(h);
    second.ws.onmessage({ data: new Uint8Array([1, 2, 3]) });
    assert.equal(second.client.connected, false);
});

regression('socket CLOSING state rejects requests without attempting WebSocket.send', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    ws.readyState = h.MockWebSocket.CLOSING;
    await assert.rejects(client.process.call('callee:compute'), error => error.code === 'connection_lost');
    assert.equal(ws.sent.length, 0);
    assert.equal(client.pending.size, 0);
    assert.equal(h.clock.timers.size, 0);
});

regression('disconnect cancels connecting, queued calls and scheduled reconnect without replay', async t => {
    const h = sandbox(t, { maxReconnectAttempts: 3, reconnectDelay: 20 });
    const client = h.create();
    const state = observe(client.connect());
    client.disconnect();
    await state.done;
    assert.equal(state.error.code, 'connection_lost');
    await h.clock.advance(1000);
    assert.equal(h.sockets.length, 1);
    const { ws } = await connect(h, client);
    const waiting = observe(client.process.call('callee:compute'));
    ws.close();
    await waiting.done;
    assert.equal(h.clock.timers.size, 1);
    client.disconnect();
    await h.clock.advance(1000);
    assert.equal(h.sockets.length, 2);
    assert.equal(h.clock.timers.size, 0);
    assert.equal(client.pending.size, 0);
});

regression('reconnect backoff survives failed handshakes and resets only after successful join', async t => {
    const h = sandbox(t, { maxReconnectAttempts: 3, reconnectDelay: 20, maxReconnectDelay: 100 });
    const { client, ws } = await connect(h);
    ws.close();
    assert.equal(client.reconnectAttempts, 1);
    await h.clock.advance(20);
    const failed = h.sockets.at(-1);
    failed.open();
    assert.equal(client.reconnectAttempts, 1, 'WebSocket open must not reset backoff');
    failed.receive({ type: 'error', request_id: failed.sent[0].request_id, payload: { code: 'denied' } });
    await checkpoint();
    assert.equal(client.reconnectAttempts, 2);
    const count = h.sockets.length;
    await h.clock.advance(39);
    assert.equal(h.sockets.length, count);
    await h.clock.advance(1);
    const success = h.sockets.at(-1);
    success.open();
    ack(success, success.sent[0]);
    await checkpoint();
    assert.equal(client.reconnectAttempts, 2);
    ack(success, success.sent[1]);
    await checkpoint();
    assert.equal(client.reconnectAttempts, 0);
    assert.equal(client.connected, true);
    assert.equal(success.sent.length, 2, 'Reconnect sends handshake only, no replay or registrations');
});

regression('failed hello/join exhaust the finite reconnect budget without orphan sockets or timers', async t => {
    const h = sandbox(t, { maxReconnectAttempts: 2, reconnectDelay: 10 });
    const client = h.create();
    const first = observe(client.connect());
    h.sockets[0].fail();
    await first.done;
    for (const delay of [10, 20]) {
        await h.clock.advance(delay);
        h.sockets.at(-1).fail();
        await checkpoint();
    }
    assert.equal(h.sockets.length, 3);
    assert.equal(client.reconnectAttempts, 2);
    assert.equal(client.connected, false);
    assert.equal(h.clock.timers.size, 0);
    assert.ok(h.sockets.every(ws => ws.readyState === h.MockWebSocket.CLOSED));
});

regression('connect timeout covers opening, hello and join under the same deadline', async t => {
    const h = sandbox(t);
    const client = h.create();
    const state = observe(client.connect());
    await h.clock.advance(60);
    const ws = h.sockets[0];
    ws.open();
    ack(ws, ws.sent[0]);
    await checkpoint();
    await h.clock.advance(40);
    await state.done;
    assert.equal(state.error.code, 'timeout');
    assert.equal(client.connected, false);
    assert.equal(h.clock.timers.size, 0);
});

for (const invalid of ['null', '[]', '{"type":[]}', '{"type":"ack","payload":[]}']) {
    regression(`malformed WS frame fails explicitly (${invalid})`, async t => {
        const h = sandbox(t);
        const { client, ws } = await connect(h);
        const state = observe(client.get('one'));
        ws.receive(invalid);
        await state.done;
        assert.equal(client.connected, false);
        assert.equal(client.pending.size, 0);
        assert.equal(h.clock.timers.size, 0);
    });
}

regression('protective limit options reject invalid values, including zero pending limits', async t => {
    const h = sandbox(t);
    for (const options of [{ maxPendingRequests: 0 }, { timeout: Infinity }, { maxBatchSize: -1 },
        { maxActiveHandlers: 0 }, { maxQueuedBytes: NaN }, { maxReconnectAttempts: -1 }]) {
        assert.throws(() => new h.Client('latzero://browser', 'pool', { autoConnect: false, ...options }), /must be/);
    }
});

regression('raw request deadline override cannot disable the finite timeout boundary', async t => {
    const h = sandbox(t);
    const { client, ws } = await connect(h);
    await assert.rejects(client.sendRequest('get_buffer', { key: 'one' }, undefined, { deadline: Infinity }), /deadline/);
    assert.equal(ws.sent.length, 0);
    assert.equal(client.pending.size, 0);
    assert.equal(h.clock.timers.size, 0);
});

regression('pool redirect advertises capability, uses distinct WS ports and resolves readiness only at final owner', async t => {
    const h = sandbox(t);
    const client = h.create({ port: 15130, wsPort: 18130, authToken: 'pool-secret' });
    const connects = [], disconnects = [];
    client.addEventListener('connect', () => connects.push(client.endpoint.url));
    client.addEventListener('disconnect', () => disconnects.push(true));
    client.on('compute', () => 42);
    const ready = client.connect();
    const state = observe(ready);
    const router = h.sockets.at(-1);
    assert.equal(router.url, 'ws://127.0.0.1:18130/');
    const request = await joinRequest(router);
    assert.deepEqual(router.sent[0].payload.capabilities, ['pool_redirect_v1']);
    router.receive(redirectMessage(request, {
        url: 'wss://evil.example/path?token=secret', path: '/admin', query: 'token=secret', auth: 'other'
    }));
    const owner = h.sockets.at(-1);
    assert.notEqual(owner, router);
    assert.equal(router.readyState, h.MockWebSocket.CLOSED);
    assert.equal(owner.url, 'ws://127.0.0.1:22130/');
    assert.equal(client.endpoint.port, 21130);
    assert.equal(client.endpoint.wsPort, 22130);
    assert.equal(Object.isFrozen(client.endpoint), true);
    assert.equal(client.port, 15130);
    assert.equal(client.wsPort, 18130);
    assert.equal(client.connect(), ready);
    assert.equal(client.connected, false);
    assert.equal(connects.length, 0);
    const joined = await joinRequest(owner);
    assert.equal(joined.client_id, 'browser');
    assert.equal(joined.pool, 'old-pool');
    assert.equal(joined.payload.auth_token, 'pool-secret');
    assert.equal(state.status, 'pending');
    assert.equal(client.connected, false);
    ack(owner, joined);
    invocation(owner, 'after-final-owner');
    await ready;
    await checkpoint();
    assert.equal(owner.sent.at(-1).payload.value, 42, 'Initial local handlers survive discovery, without advertising them');
    assert.deepEqual(connects, ['ws://127.0.0.1:22130/']);
    assert.deepEqual(disconnects, []);
    assert.equal(h.clock.timers.size, 0);
});

regression('one original deadline covers public open, hello, join and every redirect hop', async t => {
    const h = sandbox(t);
    for (const stage of ['open', 'hello', 'join', 'late-redirect']) {
        const client = h.create();
        const state = observe(client.connect());
        const router = h.sockets.at(-1);
        await h.clock.advance(30);
        router.open();
        await h.clock.advance(20);
        ack(router, router.sent[0]);
        await checkpoint();
        const request = router.sent[1];
        if (stage === 'late-redirect') {
            h.clock.now += 50; // The monotonic fence wins even if timer execution is delayed.
            router.receive(redirectMessage(request));
        } else {
            router.receive(redirectMessage(request));
            const owner = h.sockets.at(-1);
            await h.clock.advance(20);
            if (stage !== 'open') owner.open();
            if (stage === 'join') {
                ack(owner, owner.sent[0]);
                await checkpoint();
                assert.equal(owner.sent[1].type, 'join_pool');
            }
            await h.clock.advance(29);
            assert.equal(state.status, 'pending');
            await h.clock.advance(1);
        }
        await state.done;
        assert.equal(state.error.code, 'timeout', stage);
        assert.equal(client.connected, false);
        assert.equal(client._connecting, null);
        assert.equal(h.clock.timers.size, 0);
    }
});

regression('redirect cycles and configured hop limits are finite; zero disables the capability', async t => {
    const h = sandbox(t);
    for (const [options, target, expected] of [
        [{}, 14131, 'redirect_cycle'],
        [{ host: 'localhost' }, 14131, 'redirect_cycle'],
        [{}, 22130, 'redirect_cycle'],
        [{ maxRedirects: 1 }, 23130, 'redirect_limit'],
        [{ maxRedirects: 0 }, 22130, 'redirect_required']
    ]) {
        const client = h.create(options);
        const state = observe(client.connect());
        let ws = h.sockets.at(-1);
        let request = await joinRequest(ws);
        if (options.maxRedirects === 0) {
            assert.deepEqual(ws.sent[0].payload.capabilities, []);
        } else {
            ws.receive(redirectMessage(request));
            ws = h.sockets.at(-1);
            request = await joinRequest(ws);
        }
        ws.receive(redirectMessage(request, { ws_port: target }));
        await state.done;
        assert.equal(state.error.code, expected);
        assert.equal(state.error.request_id, request.request_id);
        assert.equal(client.connected, false);
    }
    const client = h.create();
    assert.equal(client.maxRedirects, 4);
    const state = observe(client.connect());
    let ws = h.sockets.at(-1);
    let request = await joinRequest(ws);
    for (let hop = 0; hop < 4; hop++) {
        ws.receive(redirectMessage(request, { ws_port: 24000 + hop }));
        ws = h.sockets.at(-1);
        request = await joinRequest(ws);
        assert.equal(state.status, 'pending');
    }
    ws.receive(redirectMessage(request, { ws_port: 24004 }));
    await state.done;
    assert.equal(state.error.code, 'redirect_limit');
    for (const maxRedirects of [-1, 17, true, 1.5, Infinity, NaN]) {
        assert.throws(() => h.create({ maxRedirects }), /maxRedirects must be/);
    }
});

regression('redirects reject unsafe hosts and remote-to-loopback SSRF while accepting canonical numeric loopback', async t => {
    const h = sandbox(t);
    const denied = [
        [{ host: 'remote.example' }, '127.0.0.1'], [{ host: '198.51.100.5' }, '127.0.0.1'],
        [{}, 'localhost'], [{}, '127.0.0.1.evil.example'], [{}, '192.168.1.2'], [{}, '0.0.0.0'],
        [{}, '127.1'], [{}, '127.00.0.1'], [{}, '0x7f000001'], [{}, '2130706433'],
        [{}, '127.0.0.1/admin'], [{}, '127.0.0.1?token=secret'], [{}, 'user@127.0.0.1'],
        [{}, '127.0.0.1\\evil'], [{}, '127.0.0.1%2fadmin'], [{}, '::ffff:127.0.0.1'],
        [{}, 'fe80::1%lo'], [{}, ' ::1'], [{}, null]
    ];
    for (const [options, host] of denied) {
        const client = h.create(options);
        const state = observe(client.connect());
        const ws = h.sockets.at(-1);
        const request = await joinRequest(ws);
        const count = h.sockets.length;
        ws.receive(redirectMessage(request, { host }));
        await state.done;
        assert.equal(state.error.code, 'unsafe_redirect', String(host));
        assert.equal(h.sockets.length, count, 'Unsafe metadata creates no additional socket');
    }
    for (const [host, expected] of [
        ['127.0.0.2', '127.0.0.2'], ['::1', '[::1]'], ['[::1]', '[::1]'], ['0:0:0:0:0:0:0:1', '[::1]']
    ]) {
        const client = h.create({ host: 'localhost' });
        const ready = client.connect();
        const ws = h.sockets.at(-1);
        ws.receive(redirectMessage(await joinRequest(ws), { host }));
        const owner = h.sockets.at(-1);
        assert.equal(owner.url, `ws://${expected}:22130/`);
        ack(owner, await joinRequest(owner));
        await ready;
    }
});

regression('redirect validates protocol, identity, pool, owner and WS ports without TCP fallback or scheme changes', async t => {
    const h = sandbox(t);
    const invalid = [
        [{ protocol: 'pool_redirect_v2' }], [{ pool: 'wrong-pool' }], [{}, { pool: 'wrong-pool' }],
        [{}, { client_id: 'other-client' }], [{ pod_count: 0 }], [{ pod_count: true }], [{ pod_count: 2.5 }],
        [{ pod_index: -1 }], [{ pod_index: 2 }], [{ pod_index: 0.5 }], [{ cluster_id: '' }],
        [{ router_host: 'remote.example' }, {}, 'unsafe_redirect'], [{ router_port: 0 }], [{ router_ws_port: '14131' }]
    ];
    for (const key of ['port', 'ws_port']) {
        for (const value of [0, 65536, -1, true, 2.5, '22130', undefined]) invalid.push([{ [key]: value }]);
    }
    invalid.push([{ ws_port: null }, {}, 'redirect_unavailable']);
    for (const [payload, envelope = {}, expected = 'invalid_redirect'] of invalid) {
        const client = h.create();
        const state = observe(client.connect());
        const router = h.sockets.at(-1);
        const request = await joinRequest(router);
        const count = h.sockets.length;
        router.receive(redirectMessage(request, payload, envelope));
        await state.done;
        assert.equal(state.error.code, expected, JSON.stringify([payload, envelope]));
        assert.equal(h.sockets.length, count);
    }
    const client = h.create({ wsProtocol: 'wss' });
    const ready = client.connect();
    const router = h.sockets.at(-1);
    assert.equal(router.url, 'wss://127.0.0.1:14131/');
    router.receive(redirectMessage(await joinRequest(router), {
        scheme: 'ws', ws_url: 'ws://evil.example/path?auth=secret', path: '/different', auth_token: 'injected'
    }));
    const owner = h.sockets.at(-1);
    assert.equal(owner.url, 'wss://127.0.0.1:22130/', 'No downgrade, upgrade or server-supplied URL fields');
    ack(owner, await joinRequest(owner));
    await ready;

    const changed = h.create();
    const state = observe(changed.connect());
    const first = h.sockets.at(-1);
    first.receive(redirectMessage(await joinRequest(first)));
    const second = h.sockets.at(-1);
    second.receive(redirectMessage(await joinRequest(second), { ws_port: 23130, cluster_id: 'another-cluster' }));
    await state.done;
    assert.equal(state.error.code, 'invalid_redirect');
    for (const options of [{ port: 0 }, { port: 65535 }, { wsPort: '14131' }, { wsPort: 65536 },
        { wsProtocol: 'https' }, { host: 'user@127.0.0.1' }, { host: '127.0.0.1/path' }]) {
        assert.throws(() => h.create(options));
    }
});

regression('owner auth denial and legacy redirect_required preserve errors with no protocol upgrade or replay', async t => {
    const h = sandbox(t);
    for (const code of ['auth_failed', 'redirect_required']) {
        const client = h.create({ authToken: 'original-auth' });
        const state = observe(client.connect());
        let ws = h.sockets.at(-1);
        let request = await joinRequest(ws);
        if (code === 'auth_failed') {
            ws.receive(redirectMessage(request));
            ws = h.sockets.at(-1);
            request = await joinRequest(ws);
            assert.equal(request.payload.auth_token, 'original-auth');
        }
        ws.receive({ type: 'error', request_id: request.request_id, payload: { code, message: 'Denied by server' } });
        ws.close(); // Immediate EOF must not replace the correlated error with connection_lost.
        await state.done;
        assert.equal(state.error.code, code);
        assert.equal(state.error.request_id, request.request_id);
        assert.equal(state.error.uncertain, true);
        assert.equal(client.connected, false);
        assert.ok(ws.sent.every(message => ['hello', 'join_pool'].includes(message.type)));
        assert.equal(h.clock.timers.size, 0);
    }
    const legacy = h.create();
    const { ws } = await connect(h, legacy); // Legacy hello ACK need not echo the new optional capability.
    assert.equal(legacy.connected, true);
    assert.equal(ws.url, 'ws://127.0.0.1:14131/');
    assert.equal(h.sockets.at(-1), ws);
});

regression('redirect is not a push or reply for other pending types, unsent work or uncorrelated IDs', async t => {
    const h = sandbox(t);
    const client = h.create();
    const ready = client.connect();
    const router = h.sockets.at(-1);
    router.open();
    router.receive(redirectMessage(router.sent[0]));
    assert.equal(client.pending.size, 1, 'Hello remains ACK-only');
    ack(router, router.sent[0]);
    await checkpoint();
    const joining = router.sent[1];
    router.receive(redirectMessage(joining, {}, { request_id: 'not-pending' }));
    assert.equal(h.sockets.at(-1), router);
    assert.equal(client.connected, false);
    ack(router, joining);
    await ready;
    router.sent.length = 0;
    const hooks = [];
    client.addEventListener('app_result', event => hooks.push(event.detail));
    for (const type of ['get_buffer', 'call_process', 'register_process', 'hello']) {
        const state = observe(client.sendRequest(type, { process_id: 'callee:echo' }));
        const request = router.sent.at(-1);
        router.receive(redirectMessage(request, { pool: 'old-pool' }, { pool: 'old-pool' }));
        await checkpoint();
        assert.equal(state.status, 'pending', type);
        assert.equal(h.sockets.at(-1), router);
        if (type === 'call_process') result(router, request, 42);
        else ack(router, request);
        await state.done;
        assert.equal(state.status, 'resolved');
    }
    router.bufferedAmount = client.maxBufferedAmount;
    const queued = observe(client.get('queued'));
    const requestId = Array.from(client.pending.keys())[0];
    router.receive(redirectMessage({ request_id: requestId, client_id: 'browser', payload: { pool: 'old-pool' } }));
    assert.equal(queued.status, 'pending');
    router.bufferedAmount = 0;
    await h.clock.advance(10);
    ack(router, router.sent.at(-1), { exists: false });
    await queued.done;
    assert.equal(hooks.length, 0);

    const nullPool = h.create({}, null);
    const nullReady = nullPool.connect();
    const nullSocket = h.sockets.at(-1);
    const nullJoin = await joinRequest(nullSocket);
    assert.equal(nullJoin.pool, null);
    assert.equal(nullJoin.payload.pool, null);
    ack(nullSocket, nullJoin);
    await nullReady;
    assert.equal(nullPool.poolName, null);
});

regression('intentional disconnect cancels every redirect continuation and reconnect timer without fencing new intent', async t => {
    const h = sandbox(t, { maxReconnectAttempts: 3, reconnectDelay: 20 });
    for (const stage of ['owner-open', 'after-hello']) {
        const client = h.create();
        const state = observe(client.connect());
        const router = h.sockets.at(-1);
        router.receive(redirectMessage(await joinRequest(router)));
        const abandoned = h.sockets.at(-1);
        if (stage === 'after-hello') {
            abandoned.open();
            ack(abandoned, abandoned.sent[0]);
        }
        client.disconnect();
        const next = client.connect();
        const current = h.sockets.at(-1);
        assert.equal(current.url, 'ws://127.0.0.1:14131/');
        abandoned.onopen?.();
        abandoned.onclose?.();
        abandoned.fail();
        ack(current, await joinRequest(current));
        await next;
        await state.done;
        assert.equal(state.error.code, 'connection_lost');
        assert.equal(client.connected, true);
        assert.equal(abandoned.sent.length, stage === 'after-hello' ? 1 : 0, 'No stale JOIN continuation');
        const count = h.sockets.length;
        client.disconnect();
        await h.clock.advance(1000);
        assert.equal(h.sockets.length, count);
        assert.equal(h.clock.timers.size, 0);
    }
    const client = h.create();
    const failed = observe(client.connect());
    h.sockets.at(-1).fail();
    await failed.done;
    assert.notEqual(client._reconnectTimer, null);
    client.disconnect();
    const count = h.sockets.length;
    await h.clock.advance(1000);
    assert.equal(h.sockets.length, count);
    assert.equal(client._reconnectTimer, null);
});

regression('old socket close or error never reconnects, resets backoff or cancels the next redirect generation', async t => {
    const h = sandbox(t, { maxReconnectAttempts: 3, reconnectDelay: 20 });
    const client = h.create();
    const errors = [], disconnects = [], connects = [];
    client.addEventListener('error', event => errors.push(event.detail));
    client.addEventListener('disconnect', () => disconnects.push(true));
    client.addEventListener('connect', () => connects.push(true));
    const failed = observe(client.connect());
    h.sockets.at(-1).fail();
    await failed.done;
    await h.clock.advance(20);
    const router = h.sockets.at(-1);
    const ready = client.connect();
    router.receive(redirectMessage(await joinRequest(router)));
    const owner = h.sockets.at(-1);
    for (let i = 0; i < 3; i++) {
        router.onclose?.();
        router.fail();
    }
    assert.equal(client.reconnectAttempts, 1);
    assert.equal(client._reconnectTimer, null);
    assert.equal(client.connect(), ready);
    const request = await joinRequest(owner);
    assert.equal(client.reconnectAttempts, 1, 'Even owner hello ACK is not final readiness');
    ack(owner, request);
    await ready;
    assert.equal(client.reconnectAttempts, 0);
    router.onclose?.();
    router.fail();
    assert.equal(client.connected, true);
    assert.equal(errors.length, 1);
    assert.equal(disconnects.length, 1);
    assert.equal(connects.length, 1);
    assert.equal(h.clock.timers.size, 0);
    client.disconnect();
    const again = client.connect();
    const entry = h.sockets.at(-1);
    assert.equal(entry.url, router.url, 'Explicit reconnect starts at the configured router, not the prior owner');
    ack(entry, await joinRequest(entry));
    await again;
    assert.equal(entry.sent.length, 2, 'Only caller-intended HELLO/JOIN, never process advertisement or effect replay');
});

regression('old asynchronous replies and result hooks stay generation-fenced through redirect reconnection', async t => {
    const h = sandbox(t);
    const { client, ws: old } = await connect(h);
    const gate = deferred();
    const hooks = [];
    client.addEventListener('app_result', event => hooks.push(event.detail));
    client.on('compute', () => gate.promise);
    invocation(old, 'stale-async-hop');
    await checkpoint();
    const pending = observe(client.process.call('callee:compute'));
    const call = old.sent.at(-1);
    const context = client._context();
    old.close();
    await pending.done;
    const ready = client.connect();
    const router = h.sockets.at(-1);
    router.receive(redirectMessage(await joinRequest(router)));
    const owner = h.sockets.at(-1);
    ack(owner, await joinRequest(owner));
    await ready;
    owner.sent.length = 0;
    gate.resolve(42);
    await checkpoint();
    result(old, call, 99);
    client.handleMessageObject({ type: 'app_result', request_id: call.request_id, payload: { value: 99 } }, context);
    client.handleMessage('invalid-old-json', context);
    assert.equal(owner.sent.length, 0);
    assert.equal(old.sent.some(message => message.request_id === 'stale-async-hop'), false);
    assert.equal(hooks.length, 0);
    assert.equal(client.connected, true);
    const current = client.process.call('callee:compute');
    const request = owner.sent.at(-1);
    result(owner, request, null, { type: 'ValueError', message: 'Application failure' });
    const envelope = await current;
    assert.equal(envelope.type, 'app_result');
    assert.deepEqual(plain(envelope.payload.error), { type: 'ValueError', message: 'Application failure' });
    result(owner, { ...request, request_id: 'unsolicited-current' }, false);
    assert.equal(hooks[0].request_id, 'unsolicited-current');
    assert.equal(hooks[0].payload.value, false);
});

regression('cross-owner switch drains old handlers, cancels old effects and rejoins with same identity and new auth', async t => {
    const h = sandbox(t);
    const { client, ws: old } = await connect(h);
    const gate = deferred();
    const registered = client.process.register(() => gate.promise, 'compute');
    ack(old, old.sent[0]);
    await registered;
    client.on('notice', () => {});
    old.sent.length = 0;
    const waiting = observe(client.process.call('callee:compute'));
    const oldCall = old.sent.at(-1);
    invocation(old, 'old-admitted-hop', 'browser:compute');
    await checkpoint();
    const seen = [];
    client.addEventListener('presence', event => seen.push(event.detail));
    const switched = observe(client.switchPool('new-pool', 'new-auth'));
    await h.clock.advance(40);
    assert.equal(old.sent.some(message => message.type === 'switch_pool'), false);
    gate.resolve('old-result');
    await checkpoint();
    const request = old.sent.at(-1);
    assert.equal(request.type, 'switch_pool');
    assert.equal(old.sent.at(-2).request_id, 'old-admitted-hop');
    assert.equal(old.sent.at(-2).pool, 'old-pool');
    old.receive({ type: 'presence_update', pool: 'old-pool', payload: { action: 'left' } });
    old.receive(redirectMessage(request));
    const owner = h.sockets.at(-1);
    old.onclose?.();
    old.fail();
    await waiting.done;
    assert.equal(waiting.error.code, 'pool_switched');
    assert.equal(waiting.error.request_id, oldCall.request_id);
    assert.equal(waiting.error.uncertain, true);
    assert.equal(client._processes.size, 0);
    assert.equal(client.eventHandlers.size, 0);
    assert.equal(client._deferredNotifications.length, 0);
    assert.equal(client.connected, false);
    assert.equal(client.poolName, 'old-pool', 'Public membership commits only at final ACK');
    const ready = client.connect();
    await h.clock.advance(20);
    const joined = await joinRequest(owner);
    assert.equal(joined.client_id, 'browser');
    assert.equal(joined.pool, 'new-pool');
    assert.equal(joined.payload.pool, 'new-pool');
    assert.equal(joined.payload.auth_token, 'new-auth');
    assert.equal(switched.status, 'pending');
    ack(owner, joined);
    invocation(owner, 'new-owner-no-advertisement', 'browser:compute', {}, 'call_app', 'new-pool');
    owner.receive({ type: 'presence_update', pool: 'new-pool', payload: { action: 'joined' } });
    await switched.done;
    await ready;
    await checkpoint();
    assert.equal(client.poolName, 'new-pool');
    assert.equal(client.authToken, 'new-auth');
    assert.equal(owner.sent.at(-1).payload.error.type, 'NoHandler');
    assert.equal(owner.sent.some(message => ['call_process', 'register_process'].includes(message.type)), false);
    assert.deepEqual(plain(seen), [{ action: 'joined' }]);
    result(old, oldCall, 'stale-result');
    assert.equal(client.connected, true);
    assert.equal(h.clock.timers.size, 0);
});

regression('switch redirect deadline, auth rejection and intentional cancellation cannot leave ambiguous or new-gen work', async t => {
    const h = sandbox(t);
    for (const outcome of ['timeout', 'auth_failed', 'disconnect']) {
        const { client, ws: old } = await connect(h);
        const state = observe(client.switchPool('new-pool', 'new-auth'));
        await checkpoint();
        await h.clock.advance(60);
        old.receive(redirectMessage(old.sent[0]));
        const owner = h.sockets.at(-1);
        await h.clock.advance(30);
        const request = await joinRequest(owner);
        if (outcome === 'timeout') await h.clock.advance(10);
        else if (outcome === 'auth_failed') {
            owner.receive({ type: 'error', request_id: request.request_id, payload: { code: 'auth_failed' } });
            owner.close();
        } else {
            client.disconnect();
            const next = client.connect();
            const current = h.sockets.at(-1);
            ack(current, await joinRequest(current));
            await next;
            ack(owner, request);
            old.onclose?.();
        }
        await state.done;
        assert.equal(state.error.code, outcome === 'disconnect' ? 'connection_lost' : outcome);
        assert.equal(client.poolName, 'old-pool');
        assert.equal(client.authToken, null);
        assert.equal(client._switching, false);
        assert.equal(client._connecting, null);
        assert.equal(client.connected, outcome === 'disconnect');
        assert.equal(h.clock.timers.size, 0);
    }
    const { client, ws } = await connect(h);
    const registered = client.process.register(() => 42, 'compute');
    ack(ws, ws.sent[0]);
    await registered;
    const waiting = observe(client.process.call('callee:compute'));
    const call = ws.sent.at(-1);
    const same = client.switchPool('old-pool');
    await checkpoint();
    ws.receive({ type: 'presence_update', pool: 'old-pool', payload: { action: 'rejoined' } });
    ack(ws, ws.sent.at(-1));
    await same;
    assert.equal(client._processes.size, 1);
    assert.equal(client.ws, ws);
    assert.equal(waiting.status, 'pending');
    result(ws, call, 7);
    await waiting.done;
    assert.equal(waiting.value.payload.value, 7);
});
