# LatZero Web Client Usage Guide

## Overview

The LatZero Web Client is a comprehensive browser-compatible client that provides both buffer operations and process pool functionality for LatZero server mode. It enables real-time communication between web clients, Node.js processes, and Python processes in the same pool.

## Features

- **Buffer Operations**: Set, get, delete, keys, values, items, mset, mget, etc.
- **Process Pool**: Register, call, broadcast, and manage distributed processes
- **Event System**: Real-time event handling and cross-process communication
- **WebSocket Connection**: Direct WebSocket connection with auto-reconnect
- **Pool-Affine Pods**: Validated local join/switch redirects with bounded hops and final-owner readiness
- **Cross-Language Compatibility**: Works seamlessly with Node.js and Python clients

## Installation

### Browser Setup

```html
<!-- Load the fixed LatZero Web Client -->
<script src="latzero-client.js"></script>
```

### Development Setup

1. Copy `latzero-client.js` to your web project
2. Include the script in your HTML file
3. Start the LatZero server with an explicit browser origin, for example `latzero-server --ws-origin http://127.0.0.1:8080`. For a local file demo, opt in to the `null` origin string with `--ws-origin null`.
4. Open your HTML file in a browser

## Quick Start

```javascript
// Create client instance
const client = new LatZeroWebClient('latzero://my-web-client', 'my-pool', {
    host: '127.0.0.1',
    port: 14130,
    // wsPort: 14131, // Override if the public WS listener is not TCP + 1
    maxRedirects: 4,
    autoConnect: true
});

// Set up event listeners
client.addEventListener('connect', () => {
    console.log('Connected to LatZero server');
});

client.addEventListener('disconnect', () => {
    console.log('Disconnected from server');
});

client.addEventListener('error', (event) => {
    console.error('Connection error:', event.detail.message);
});
```

## API Reference

### Constructor

```javascript
new LatZeroWebClient(dsn, pool, options)
```

**Parameters:**
- `dsn` (string): Client DSN in format `latzero://client-id`
- `pool` (string): Pool name to join
- `options` (object): Optional configuration; full protective limits are listed in `README.md`.
- `host` (string): Server host (default: `'127.0.0.1'`).
- `port` (number): Public TCP port (default: `14130`).
- `wsPort` (number): Explicit public WebSocket port (default: `port + 1`), integer 1..65535.
- `wsProtocol` (string): `'ws'` (default) or `'wss'`, preserved across redirects without TLS upgrades/downgrades.
- `maxRedirects` (number): Bounded join/switch hops (default: `4`, integer 0..16); `0` disables the capability and following.
- `timeout` (number): One total deadline in milliseconds for queued send, ACK and RPC result (default: `5000`).
- `autoConnect` (boolean): Auto-connect on creation (default: `true`); await `client.connect()` before requests.
- `maxReconnectAttempts` (number): Connection-only reconnect budget (default: `5`, `0` disables).
- `reconnectDelay` / `maxReconnectDelay` (number): Initial/capped backoff milliseconds (defaults: `1000` / `30000`), with jitter; resets only after successful hello/join.

### Pod Mode

Use this SDK revision for a server explicitly started using `--pods N`. Hello
advertises `pool_redirect_v1`; only a sent, correlated `join_pool` or `switch_pool`
can redirect. Unrelated request types and unsolicited redirects cannot move the
socket or consume pending work. Older SDKs receive `redirect_required`; ordinary
classic-server ACKs remain supported without automatically upgrading protocols.

Configure the public entry point, not individual pods. Its `host`, TCP `port` and
`wsPort` remain stable for a future explicit reconnect. Read-only `client.endpoint`
is the frozen actual owner socket (`host`, `port`, `wsPort`, `protocol`, `url`), or
`null` after disconnect. Owner discovery uses only the reply's explicit `ws_port`,
never TCP + 1 as a fallback or arbitrary URLs, paths, query strings or credentials.

The configured host must already be numeric loopback or `localhost` to follow.
Targets must be numeric loopback (canonical IPv4 127/8 or IPv6 `::1`), never
hostnames or private/remote addresses. Protocol, exact requested client/pool,
owner index/count, ports, stable cluster metadata and visited endpoints are
validated. Four hops are allowed by default, bounded by `maxRedirects`. A local
router is still trusted to select local ports; discovery is not authentication.
Use a trusted entry and supply pool auth explicitly. The final owner checks auth
again and its denial is returned unchanged, without retrying auth or replaying work.

### Buffer Operations

#### set(key, value, options)
Store a value in the buffer.

`autoClean` is nonnegative finite milliseconds, converted to fractional wire seconds. `30000` is 30 seconds, `125` is 0.125 seconds, `0` expires immediately, and null/omitted means no TTL. This corrects the previous accidental milliseconds-as-seconds behavior. Optional `timeout` is milliseconds.

```javascript
await client.set('user:123', { name: 'John', age: 30 });
await client.set('temp:data', 'expires soon', { autoClean: 30000 });
await client.set('config', { debug: true }, { persistent: true });
```

#### get(key, defaultValue)
Retrieve a value from the buffer.

```javascript
const user = await client.get('user:123');
const config = await client.get('config', { debug: false });
```

#### delete(key)
Delete a key from the buffer.

```javascript
const deleted = await client.delete('user:123');
```

#### exists(key)
Check if a key exists.

```javascript
const exists = await client.exists('user:123');
```

#### keys(pattern)
List all keys matching a pattern.

```javascript
const allKeys = await client.keys();
const userKeys = await client.keys('user:*');
```

#### values(pattern)
Get all values for keys matching a pattern.

```javascript
const allValues = await client.values();
const userValues = await client.values('user:*');
```

#### items(pattern)
Get key-value pairs for keys matching a pattern.

```javascript
const allItems = await client.items();
const userItems = await client.items('user:*');
```

#### mset(data, options)
Set multiple key-value pairs.

Batch item and concurrent-batch limits also apply to `mget`, `deleteMany`, `values` and `items`. Batches are not atomic: accepted effects are not rolled back when another item fails; the batch slot remains occupied until every started request settles. Oversized batches reject before item requests are sent.

```javascript
await client.mset({
    'user:123': { name: 'John' },
    'user:456': { name: 'Jane' },
    'config': { debug: true }
});
```

#### mget(keys)
Get multiple values.

```javascript
const values = await client.mget(['user:123', 'user:456', 'config']);
```

#### deleteMany(keys)
Delete multiple keys.

```javascript
const deletedCount = await client.deleteMany(['user:123', 'user:456']);
```

#### size()
Get the number of keys in the buffer.

```javascript
const count = await client.size();
```

#### stats()
Get pool statistics.

```javascript
const stats = await client.stats();
console.log(stats);
// {
//   name: 'my-pool',
//   client_id: 'my-web-client',
//   server_mode: true,
//   key_count: 42
// }
```

#### scan(cursor, count)
Paginate through keys.

```javascript
const [nextCursor, keys] = await client.scan(0, 100);
```

### Process Pool Operations

#### client.process.register(fn, nameOverride)
Register a function as a named process.

The handler is prepared/replaced before advertising the registration; registration failure restores the prior handler. Browser handlers run on the main thread, not in threads or WebWorkers, regardless of legacy `workerKind` metadata. Async functions are awaited. Re-registering a name replaces rather than appends the old callback.

```javascript
// Register an add function
await client.process.register(function(data) {
    const { a, b } = data;
    return a + b;
}, 'add');

// Register with explicit name (for anonymous functions)
await client.process.register((data) => {
    return data.x * data.y;
}, 'multiply');
```

#### client.process.call(processId, data, options)
Call a specific process by ID.

Direct/self calls (including `responseTo: client.clientId`) return a terminal `app_result` envelope with `payload.value` and `payload.error`, in either ACK/result order. Application errors resolve as `{value: null, error: {...}}` inside the payload rather than rejecting. Transport/protocol failures reject. An explicit different `responseTo` returns acceptance ACK metadata instead; that recipient observes the full unsolicited envelope through its DOM `app_result` hook.

```javascript
const result = await client.process.call('other-client:add', { a: 5, b: 3 });
console.log(result.payload.value); // 8

// With timeout
const result = await client.process.call('client:process', { x: 10 }, { timeout: 10000 });

// On the result-recipient client:
client.addEventListener('app_result', event => {
    const envelope = event.detail;
    console.log(envelope.request_id, envelope.payload.value, envelope.payload.error);
});

// On the caller client:
const accepted = await client.process.call('client:process', { x: 10 }, {
    responseTo: 'result-recipient', timeout: 10000
});
console.log(accepted.type, accepted.request_id); // 'ack', original caller ID
```

#### client.process.broadcast(processName, data, options)
Broadcast to all processes with a given name.

This returns the accepted process IDs, not executions. Each child result is an unsolicited `app_result` with its own opaque child ID and optional parent-correlation metadata, delivered to the caller by default or explicit `options.responseTo`. `options.timeout` is milliseconds.

```javascript
const invoked = await client.process.broadcast('add', { a: 5, b: 3 });
console.log(`Invoked ${invoked.length} processes: ${invoked.join(', ')}`);
```

#### client.process.list(pattern)
List all registered processes.

```javascript
const allProcesses = await client.process.list();
const myProcesses = await client.process.list('my-client:*');
```

#### client.process.unregister(name)
Unregister a process.

```javascript
await client.process.unregister('add');
```

### Event System

#### on(event, handler)
Register an event handler.

Use `on`/`off` for application notifications/RPC callbacks. Use DOM `addEventListener`/`removeEventListener` for lifecycle, presence, buffer and unsolicited `app_result` observations. RPC invokes the first matching event handler; async notification handlers run in registration order with failures isolated. Handler admission is finite: excess RPC receives a safe `Overloaded` application error; notification overload explicitly closes the connection rather than silently losing work.

```javascript
client.on('user-updated', (data) => {
    console.log('User updated:', data);
    updateUI(data);
});
```

#### off(event, handler)
Remove an event handler.

```javascript
const handler = (data) => console.log(data);
client.on('test', handler);
client.off('test', handler);
```

#### emitEvent(event, options)
Emit a fire-and-forget event.

```javascript
await client.emitEvent('user-updated', {
    data: { userId: 123, name: 'John' },
    targetClientId: 'admin-client'
});
```

#### callEvent(event, options)
Emit an RPC-style event with response.

Requires `targetClientId` and uses the same terminal/third-party and millisecond deadline rules as `process.call`. There is one request ID and pending promise, not a polling loop.

```javascript
const response = await client.callEvent('get-user-info', {
    targetClientId: 'user-service',
    data: { userId: 123 }
});
console.log(response.payload.value, response.payload.error);
```

### Connection Management

#### connect()
Connect to the server.

Concurrent calls share one connection promise. Readiness starts only after hello ACK and join ACK; opening the WebSocket alone is not readiness. Failed handshake/open is fenced, timers are cleared, and no effectful requests or process registrations are automatically replayed on reconnect.

In pod mode this promise and the original timeout cover public WS opening, hello,
join and all redirects. `connected` becomes true only at the final owner ACK.
Intermediate closure does not schedule reconnect, reset backoff or cancel the new
owner. Later explicit `connect()` uses the configured public entry again.

```javascript
await client.connect();
```

#### disconnect()
Disconnect from the server.

Cancels pending waiters/unsent frames, closes the socket and cancels/disables auto-reconnect until explicit `connect()`. No unobserved leave-pool request is created.
All redirect continuations are canceled as well; an old continuation cannot send a
join or cancel a new explicitly connected generation.

```javascript
client.disconnect();
```

#### switchPool(pool, authToken)
Switch to a different pool.

New requests/handlers pause while admitted handlers drain under the same total deadline used for the switch ACK. Replies retain their old pool/socket/generation. A successful ACK commits membership synchronously before further messages, cancels old-pool waiters, and clears old-pool application handlers/process registrations. Re-register explicitly in the new pool. A pre-send quiescence timeout or explicit protocol rejection preserves the old context; an uncertain transmitted switch timeout closes the connection instead of guessing membership.

A same-pool switch is an idempotent rejoin: registrations, application handlers and pending routes remain intact.

If the new pool has a different pod owner, the SDK quiesces the old context and
performs hello/join on the new owner with the same client ID and requested auth.
The original deadline includes draining, the switch request and every new handshake
hop. Readiness and the switch promise complete only at the final owner ACK. Old
pending work is rejected with the existing `code`, `request_id`, `uncertain` fields;
old asynchronous replies cannot reach the new connection. Registrations are not
re-advertised, and effect RPCs are never replayed. Re-register explicitly after the
membership change.

```javascript
await client.switchPool('new-pool', 'auth-token');
```

## Cross-Process Communication

### Web Client to Python Process

```javascript
// Python process registers 'calculate' function
// Web client calls it
const result = await client.process.call('python-client:calculate', {
    x: 10, y: 20
});
```

### Python Process to Web Client

```python
# Python sends call_app message
{
    "type": "call_app",
    "event": "web-client:add",
    "data": {"x": 5, "y": 3}
}
```

```javascript
// Web client registers the handler
await client.process.register(function(data) {
    const a = data.a !== undefined ? data.a : data.x;
    const b = data.b !== undefined ? data.b : data.y;
    return a + b;
}, 'add');
```

## Event Handling

### Server Events

```javascript
client.addEventListener('presence', (event) => {
    console.log('Client joined/left:', event.detail);
});

client.addEventListener('bufferUpdate', (event) => {
    console.log('Buffer changed:', event.detail);
    refreshData();
});
```

### Custom Events

```javascript
// Register handler
client.on('notification', (data) => {
    showNotification(data.message, data.type);
});

// Emit from another client
await client.emitEvent('notification', {
    data: { message: 'Task completed!', type: 'success' },
    targetClientId: 'ui-client'
});
```

## Error Handling

```javascript
try {
    await client.set('key', 'value');
} catch (error) {
    if (error.code === 'timeout') {
        console.log('Request timed out');
    } else {
        console.error('Operation failed:', error.message);
    }
}

// Global error handling
client.addEventListener('error', (event) => {
    console.error('Client error:', event.detail.message);
});
```

## Best Practices

### Process Registration

```javascript
// Use explicit names for anonymous functions.
await client.process.register((data) => {
    return data.x * data.y;
}, 'multiply');

// Handle both data formats when existing senders require it.
await client.process.register(function(data) {
    const a = data.a !== undefined ? data.a : data.x;
    const b = data.b !== undefined ? data.b : data.y;
    return a + b;
}, 'add');

// Avoid anonymous functions without explicit names.
await client.process.register((data) => data.a + data.b); // Will fail
```

### Connection Management

```javascript
// Handle connection states; any re-registration here is an explicit application choice.
client.addEventListener('connect', () => {
    console.log('Connected, registering processes...');
    registerProcesses();
});

client.addEventListener('disconnect', () => {
    console.log('Disconnected, pausing operations...');
    pauseOperations();
});

// Intentional shutdown disables reconnect.
window.addEventListener('beforeunload', () => {
    client.disconnect();
});
```

### Error Recovery

Do not automatically replay an effectful request after timeout or disconnect. A request that reached `WebSocket.send` may have executed even without an observed result; `error.uncertain` marks that possibility and `error.request_id` retains correlation. A queued request canceled before send is removed and will not transmit later. Application-level reconciliation/idempotency is a separate contract.

On reconnect, re-read buffer state and explicitly re-establish required registrations/subscriptions. Live notifications are not a durable replay log. Synchronous CPU-heavy callbacks still block the browser event loop, and a never-settling promise keeps its bounded admission slot; this release adds no WebWorkers or preemptive cancellation.

## Demo Files

### index.html
Complete demo showcasing:
- Process registration and calling
- Buffer operations
- Cross-process communication
- Real-time event handling
- Server TUI integration

Open `index.html` in your browser to try the demo; it is not an automated conformance test.

## Troubleshooting

### Common Issues

**"Process not found in server TUI"**
- Ensure you're using `latzero-client.js` (fixed version)
- Check that the client successfully connected
- Verify process registration completed without errors

**"Cross-process calls failing"**
- Ensure both clients are in the same pool
- Check process ID format: `client-id:process-name`
- Verify data format compatibility

**"Connection issues"**
- Check server is running on correct host/port
- Verify `wsPort`, or the default TCP port + 1, is the public WebSocket listener
- Authorize the exact HTTP page origin, or explicitly the `null` origin for a file page
- Check for firewall issues

**"Origin or mixed-content rejection"**
- Allow the page's exact origin on every pod, including its page port when present, for example `http://127.0.0.1:8080`
- The page Origin stays unchanged across WS endpoints; ephemeral pod destination ports are not additional origins
- A direct `file://` page sends `Origin: null`; the user must explicitly opt in to that literal origin string
- Local pod mode is plain WS, not WSS; the SDK creates no TLS server and cannot bypass HTTPS-page mixed-content restrictions
- `wsProtocol: 'wss'` only works with an existing TLS-capable endpoint and never downgrades on redirect

### Debug Mode

```javascript
// Enable detailed logging
client.addEventListener('connect', () => {
    console.log('Connected successfully');
});

client.addEventListener('error', (event) => {
    console.error('Connection error:', event.detail);
});

// Log all messages
client.addEventListener('presence', (event) => {
    console.log('Presence:', event.detail);
});
```

## Server Integration

The web client integrates seamlessly with the LatZero server TUI:

- **Processes Tab**: Shows all registered processes with their owners
- **Clients Tab**: Displays connected web clients
- **Buffers Tab**: Shows stored key-value pairs
- **Events Tab**: Real-time event log

Ensure processes appear in the server TUI to verify proper registration and visibility.

## Regression Tests

Run `node --unhandled-rejections=strict --test latzero-client.test.js` without
external installs. All original 65 cases remain, with 12 new redirect regression
groups. The actual script runs in a VM with mocked WebSockets and deterministic
timers, covering security, one deadline across hops, cancellation, stale generations,
owner-switch auth and no replay alongside the existing protocol/lifetime cases.

`latzero-client.integration.test.js` adds an opt-in native Node WebSocket test for
an already-running disposable pod fixture. It requires the explicit
`LATZERO_WEB_POD_TEST` JSON configuration documented in `README.md`; it never
starts a server or chooses default ports/data. Without that fixture or native
WebSocket it skips. Actual browser Origin/mixed-content behavior and real browser
bufferedAmount still require a separately authorized native-page gate; Node WS/VM
tests and the demo do not substitute for it.
