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
    const create = extra => {
        const client = new Client('latzero://browser', 'old-pool', {
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
