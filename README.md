# Eau2

Distributed key-value store that supports distributed dataframes and dataframe operations.

# make targets

## Run all unit tests
Run all tests that do not involve any networking.
```bash
make test
```

## Run milestone 2
Testing the key value store on a single node (no networking). Checks putting and getting Dataframes from a single key-value store.

```bash
make milestone2
```


## Run milestone 3
Testing the key value store on multiple nodes with known key values. This will run the networking server in the background as well as three applications in separate processes. Should print `Milestone3: SUCCESS`.
```bash
make milestone3
```

## Manual tests
### Run client test
This will create a client process with a CLI that allows for `get`, `getAndWait`, and `put` messages to be exchanged with other clients. To send the message type `<message type> <node_index> <message>`. Example: `get 1 Hello World`.

First run:
```bash
make server
```
Then in another terminal:
```bash
make client
```

### Run Key Value Store test
This will create a KVStore process with a CLI that allows for `get`, `getAndWait`, and `put` messages to be exchanged with other clients. To send the message type `<get message type> <node_index> <key>` or `put <node_index> <key> <value>`. Example: `get 1 some_key` or `put 0 foo value_to_put`.

First run:
```bash
make server
```
Then in another terminal:
```bash
make kvstore
```
