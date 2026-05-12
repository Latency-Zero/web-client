# LatZero Web Client

A browser-compatible client for LatZero server mode that works with simple script tags.

## Quick Start

Include the client in your HTML:

```html
<script src="latzero-web.js"></script>
```

## Usage

```javascript
// Create client
const client = new LatZeroWebClient('latzero://my-web-client', 'my-pool', {
    host: '127.0.0.1',
    port: 14130,
    authToken: null, // optional
    timeout: 5000, // request timeout in ms
    maxReconnectAttempts: 5, // auto-reconnect settings
    reconnectDelay: 1000
});

// Wait for connection
client.addEventListener('connect', () => {
    console.log('Connected to LatZero server');
});

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

// Register event handlers
client.addEventListener('compute:multiply', (event) => {
    const { x, y } = event.detail;
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
console.log(result); // 42

// Cleanup
client.disconnect();
```

## Demo

Open `index.html` in a web browser to see a complete interactive demo of the web client functionality.

## API Reference

### Constructor
- `new LatZeroWebClient(dsn, pool, options)`

**Parameters:**
- `dsn` (string): Client DSN in format `latzero://client-id`
- `pool` (string): Pool name to join
- `options` (object): Optional configuration
  - `host` (string): Server host (default: '127.0.0.1')
  - `port` (number): Server port (default: 14130)
  - `authToken` (string): Optional authentication token
  - `timeout` (number): Request timeout in ms (default: 5000)
  - `autoConnect` (boolean): Auto-connect on creation (default: true)
  - `maxReconnectAttempts` (number): Max reconnection attempts (default: 5)
  - `reconnectDelay` (number): Delay between reconnections in ms (default: 1000)

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

### Event Operations
- `addEventListener(event, handler)` - Register event handler
- `removeEventListener(event, handler)` - Remove event handler
- `emitEvent(event, options)` - Emit fire-and-forget event
- `callEvent(event, options)` - Emit RPC-style event with response

### Connection Management
- `connect()` - Connect to server (called automatically unless autoConnect=false)
- `disconnect()` - Disconnect from server
- `switchPool(pool, authToken)` - Switch to different pool

### Built-in Events
- `connect` - Client connected to server
- `disconnect` - Client disconnected from server
- `error` - Connection or protocol error
- `presence` - Client presence updates
- `bufferUpdate` - Buffer change notifications

## Browser Compatibility

The web client uses WebSocket for browser compatibility. It requires:
- Modern browser with WebSocket support
- ES6+ JavaScript support
- No external dependencies

## Notes

- The client uses WebSocket instead of TCP for browser compatibility
- Auto-reconnection is built-in with configurable attempts and delays
- All data is automatically JSON serialized/deserialized
- Event handlers use standard DOM EventTarget API
