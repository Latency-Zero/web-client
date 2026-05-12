# LatZero Web Client Usage Guide

## Overview

The LatZero Web Client is a comprehensive browser-compatible client that provides both buffer operations and process pool functionality for LatZero server mode. It enables real-time communication between web clients, Node.js processes, and Python processes in the same pool.

## Features

- **Buffer Operations**: Set, get, delete, keys, values, items, mset, mget, etc.
- **Process Pool**: Register, call, broadcast, and manage distributed processes
- **Event System**: Real-time event handling and cross-process communication
- **WebSocket Connection**: Direct WebSocket connection with auto-reconnect
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
3. Start the LatZero server: `latzero-server`
4. Open your HTML file in a browser

## Quick Start

```javascript
// Create client instance
const client = new LatZeroWebClient('latzero://my-web-client', 'my-pool', {
    host: '127.0.0.1',
    port: 14130,
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
- `options` (object): Optional configuration
  - `host` (string): Server host (default: '127.0.0.1')
  - `port` (number): Server port (default: 14130)
  - `timeout` (number): Request timeout in ms (default: 5000)
  - `autoConnect` (boolean): Auto-connect on creation (default: true)
  - `maxReconnectAttempts` (number): Max reconnect attempts (default: 5)
  - `reconnectDelay` (number): Reconnect delay in ms (default: 1000)

### Buffer Operations

#### set(key, value, options)
Store a value in the buffer.

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

```javascript
const result = await client.process.call('other-client:add', { a: 5, b: 3 });
console.log(result.payload.value); // 8

// With timeout
const result = await client.process.call('client:process', { x: 10 }, { timeout: 10000 });
```

#### client.process.broadcast(processName, data, options)
Broadcast to all processes with a given name.

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

```javascript
const response = await client.callEvent('get-user-info', {
    targetClientId: 'user-service',
    data: { userId: 123 }
});
```

### Connection Management

#### connect()
Connect to the server.

```javascript
await client.connect();
```

#### disconnect()
Disconnect from the server.

```javascript
client.disconnect();
```

#### switchPool(pool, authToken)
Switch to a different pool.

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
    if (event.detail.code === 'connection_lost') {
        // Implement reconnection logic
        setTimeout(() => client.connect(), 5000);
    }
});
```

## Best Practices

### Process Registration

```javascript
// ✅ Good: Use explicit names for anonymous functions
await client.process.register((data) => {
    return data.x * data.y;
}, 'multiply');

// ✅ Good: Handle both data formats for compatibility
await client.process.register(function(data) {
    const a = data.a !== undefined ? data.a : data.x;
    const b = data.b !== undefined ? data.b : data.y;
    return a + b;
}, 'add');

// ❌ Avoid: Anonymous functions without explicit names
await client.process.register((data) => data.a + data.b); // Will fail
```

### Connection Management

```javascript
// ✅ Good: Handle connection states
client.addEventListener('connect', () => {
    console.log('Connected, registering processes...');
    registerProcesses();
});

client.addEventListener('disconnect', () => {
    console.log('Disconnected, pausing operations...');
    pauseOperations();
});

// ✅ Good: Graceful shutdown
window.addEventListener('beforeunload', () => {
    client.disconnect();
});
```

### Error Recovery

```javascript
// ✅ Good: Implement retry logic
async function robustSet(key, value, maxRetries = 3) {
    for (let i = 0; i < maxRetries; i++) {
        try {
            await client.set(key, value);
            return;
        } catch (error) {
            if (i === maxRetries - 1) throw error;
            await new Promise(resolve => setTimeout(resolve, 1000 * (i + 1)));
        }
    }
}
```

## Demo Files

### demo-fixed.html
Complete demo showcasing:
- Process registration and calling
- Buffer operations
- Cross-process communication
- Real-time event handling
- Server TUI integration

Open `demo-fixed.html` in your browser to see all features in action.

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
- Verify WebSocket server is on port+1
- Check for firewall issues

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
