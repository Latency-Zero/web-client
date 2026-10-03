# LatZero Web Client

A browser-compatible client for LatZero server mode that works with simple script tags.

## Quick Start

Include the client in your HTML:

```html
<script src="latzero-client.js"></script>
```

## Usage

```javascript
// Create client
const client = new LatZeroWebClient('latzero://my-web-client', 'my-pool', {
    host: '127.0.0.1',
    port: 14130,
    authToken: null, // optional
    timeout: 5000, // total request deadline in ms, including queued send and RPC completion
    maxReconnectAttempts: 5, // auto-reconnect settings
    reconnectDelay: 1000
});

// Wait for connection
client.addEventListener('connect', () => {
    console.log('Connected to LatZero server');
});
await client.connect(); // Shares the auto-connect promise; resolves after hello and join ACKs.

// Handle connection events
client.addEventListener('disconnect', () => {
    console.log('Disconnected from server');
});

client.addEventListener('error', (event) => {
    console.error('Connection error:', event.detail);
});

// Basic key-value operations
await client.set('user', { name: 'Alice', age: 30 });
const user = await client.get('user');
console.log(user); // { name: 'Alice', age: 30 }

const keys = await client.keys();
console.log(keys); // ['user']

await client.delete('user');
console.log(await client.exists('user')); // false

// Batch operations
await client.mset({
    'key1': 'value1',
    'key2': 'value2'
});

const values = await client.mget(['key1', 'key2']);
console.log(values); // { key1: 'value1', key2: 'value2' }

// Event handling
client.addEventListener('presence', (event) => {
    console.log('Presence update:', event.detail);
});

client.addEventListener('bufferUpdate', (event) => {
    console.log('Buffer update:', event.detail);
});

// Register an RPC handler. DOM addEventListener is for lifecycle/result notifications.
client.on('compute:multiply', async ({ x, y }) => {
    return x * y;
});

// Emit events
await client.emitEvent('user:login', {
    data: { username: 'alice' }
});

// Call events (RPC)
const result = await client.callEvent('compute:multiply', {
    targetClientId: 'other-client',
    data: { x: 7, y: 6 }
});
console.log(result.payload); // { value: 42, error: null, ...routing metadata }

// Explicit third-party routing returns acceptance to this caller, not execution success.
const accepted = await client.process.call('other-client:multiply', { x: 7, y: 6 }, {
    responseTo: 'result-client'
});
console.log(accepted.type, accepted.request_id); // 'ack', original request ID

// Install this on result-client. The detail is the full wire envelope.
client.addEventListener('app_result', event => {
    console.log(event.detail.request_id, event.detail.payload.value, event.detail.payload.error);
});

// Cleanup
client.disconnect();
```

## Demo

Open `index.html` in a web browser to see a complete interactive demo of the web client functionality.

Configure the daemon to allow the page's exact origin. For a local `file://` demo, opt in explicitly with `latzero-server --ws-origin null`; `null` is an origin string, not a trusted browser identity. For an HTTP page, use its actual origin, for example `--ws-origin http://127.0.0.1:8080`. The browser connects to `ws://host:(port + 1)`; HTTPS/mixed-content and remote TLS are outside this client phase.

## API Reference

### Constructor
- `new LatZeroWebClient(dsn, pool, options)`

**Parameters:**
- `dsn` (string): Client DSN in format `latzero://client-id`
- `pool` (string): Pool name to join
- `options` (object): Optional configuration, described below.

| Option | Default | Meaning |
| --- | --- | --- |
| `host` | `'127.0.0.1'` | WebSocket host |
| `port` | `14130` | TCP base port; browser uses this port plus one |
| `authToken` | `null` | Optional pool authentication token |
| `timeout` | `5000` | Total request/connect/switch deadline in milliseconds |
| `autoConnect` | `true` | Start connection on construction |
| `maxReconnectAttempts` | `5` | Finite connection-only retry budget; `0` disables it |
| `reconnectDelay` | `1000` | Initial reconnect delay in milliseconds, with exponential backoff and jitter |
| `maxReconnectDelay` | `30000` | Reconnect delay cap in milliseconds |
| `maxPendingRequests` | `256` | Pending ACK/result waiter limit |
| `maxBatchSize` | `128` | Items per batch, including `values`/`items` listed keys |
| `maxPendingBatches` | `8` | Concurrent batch operations |
| `maxActiveHandlers` | `32` | Admitted RPC/notification handler tasks |
| `maxQueuedMessages` | `256` | Unsent regular frames |
| `maxQueuedBytes` | `4194304` | Unsent regular UTF-8 JSON bytes |
| `maxBufferedAmount` | `1048576` | WebSocket `bufferedAmount` plus next frame ceiling |
| `maxFrameBytes` | `1048576` | Incoming/outgoing JSON frame ceiling |
| `sendPollInterval` | `10` | Milliseconds between bounded bufferedAmount drain checks |

Counts/byte budgets must be positive safe integers; durations must be positive finite milliseconds no greater than `2147483647`. RPC replies have an additive queue reserve of `maxActiveHandlers` frames and `maxFrameBytes` bytes. These are protective defaults, not measured capacity claims. A frame must also fit the WebSocket send budget.

### Key-Value Operations
- `set(key, value, options)` - Set a key with optional TTL and persistence
- `get(key, defaultValue)` - Get a value, return default if not found
- `delete(key)` - Delete a key
- `exists(key)` - Check if key exists
- `keys(pattern)` - List keys, optional pattern filtering
- `values(pattern)` - Get all values, optional pattern filtering
- `items(pattern)` - Get key-value pairs, optional pattern filtering
- `mset(data, options)` - Set multiple keys
- `mget(keys)` - Get multiple keys
- `deleteMany(keys)` - Delete multiple keys
- `size()` - Get number of keys
- `stats()` - Get client and pool statistics
- `scan(cursor, count)` - Paginated key scanning

`set`/`mset` `options.autoClean` is milliseconds, converted to fractional wire seconds (`30000` becomes `30`, `125` becomes `0.125`); `0` expires immediately and null/omitted means no TTL. `options.timeout` on `set`/`mset` is milliseconds. Batches are not atomic and do not roll back already accepted effects; their admission slot stays occupied until all started requests settle.

### Event Operations
- `on(event, handler)` / `off(event, handler)` - Register/remove application handlers; RPC invokes the first handler, notification messages await handlers in registration order
- `addEventListener(event, handler)` / `removeEventListener(event, handler)` - Observe DOM lifecycle, presence, buffer and result events
- `emitEvent(event, options)` - Emit fire-and-forget event
- `callEvent(event, options)` - Emit RPC-style event with response

`callEvent` requires `options.targetClientId`. `process.call` and `callEvent` accept `options.timeout` in milliseconds. Default/direct/self calls, including `responseTo: client.clientId`, resolve the terminal `app_result` envelope, not acceptance ACK. Explicit `responseTo` naming another client resolves the acceptance ACK envelope. Application failures resolve normally with `payload.value === null` and `payload.error`; transport/protocol errors reject with `error.code`. Incoming hop IDs are opaque and echoed unchanged; public envelopes preserve the original request ID and any additive `payload.request_id` correlation.

### Connection Management
- `connect()` - Connect to server (called automatically unless autoConnect=false)
- `disconnect()` - Cancel pending work, close the socket and disable/cancel automatic reconnect until explicit `connect()`
- `switchPool(pool, authToken)` - Pause admission, drain admitted handlers within one deadline, then switch; success clears old-pool application handlers/process registrations

Same-pool switching is an idempotent rejoin and retains registrations and pending routes.

Old-pool notifications received after switch transmission are deferred within `maxQueuedMessages`/`maxQueuedBytes`, released on rejection/same-pool ACK, and fenced on a successful membership change. This accounts for presence updates emitted before the switch ACK without executing work against ambiguous membership.

### Built-in Events
- `connect` - Client connected to server
- `disconnect` - Client disconnected from server
- `error` - Connection or protocol error
- `presence` - Client presence updates
- `bufferUpdate` - Buffer change notifications
- `app_result` - Unsolicited RPC/broadcast/third-party result; `event.detail` is the complete envelope, not only its payload

## Browser Compatibility

The web client uses WebSocket for browser compatibility. It requires:
- Modern browser with WebSocket support
- Modern JavaScript support including the existing async/await and optional-chaining syntax, plus built-in EventTarget, CustomEvent, TextEncoder and performance.now
- No external dependencies

## Notes

- The client uses WebSocket instead of TCP for browser compatibility
- Auto-reconnection is connection-only, with finite attempts and bounded backoff; backoff resets only after successful hello/join
- All data is automatically JSON serialized/deserialized
- Application handlers run on the browser main thread; promises are awaited, but CPU-blocking code cannot be preempted and a never-settling handler retains an admission slot
- Async replies retain their incoming pool/socket/generation; disconnected or superseded work never replies through a new socket/pool
- After a successful switch or reconnect, re-register processes explicitly; subscriptions/notifications are not replayed, so re-read state after reconnect

## Stabilization Release Notes

- Typed pending correlation no longer consumes pushes/self invocations with shared IDs. Process/event RPC waits for terminal results in either ACK/result order; `callEvent` no longer polls nonexistent pending keys.
- Handler promises are awaited. Exceptions, circular/BigInt/non-JSON results and serialization failures produce safe `{value,error}` application replies. Re-registering replaces the handler before advertising and restores it on registration failure.
- `autoClean` now honors the documented milliseconds API. Applications relying on the old accidental seconds interpretation must update their values.
- One monotonic deadline covers queued send, ACK and result, with timers/unsent frames cleaned up on settlement. Socket state/UTF-8 byte budgets, pending/batch/handler limits and explicit overload failures replace unbounded paths. A transmitted timeout/disconnect can still have had effects (`error.uncertain`); no automatic request retry, replay or exactly-once guarantee is added.
- Intentional disconnect prevents reconnect. A transmitted switch timeout closes the connection rather than retaining ambiguous remote membership. No WebWorkers or new remote transport contract are introduced.

## Tests

Run `node --test latzero-client.test.js`. The dependency-free suite evaluates the actual browser script in a Node VM using deterministic WebSocket mocks, fake monotonic timers, explicit promise/event-loop barriers and bounded test deadlines. It installs nothing and uses no daemon, default cache or live ports.

Real-browser WebSocket integration is a separate, unrun gate: use an explicitly origin-authorized daemon with temporary data/ports and verify browser-to-Python/Node self/direct/third-party RPC, close/backpressure and pool-switch behavior. The mock suite and `index.html` demo do not substitute for that gate.
