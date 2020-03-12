# Milestone 1

# Introduction
The eua2 system is a system that allows users to run applications involving dataframes built on a distributed key-value store. The system can be used to map and filter through large dataframes that do not fit in the memory of a single machine.  

# Architecture

## Network and Key-Value Store Layer

This layer is a distributed KV store running on multiple node. Each KV store node has part of the data, and the KV store nodes talk to exchange data when needed. All of the networking and concurrency control is hidden here.

### Server and Client startup/setup
To setup the system, the registration server must first be started. Clients (KV Store nodes) will start after and connect to the server. The server keeps track of all registered clients and broadcasts all registered clients to each of the registered clients. This allows the clients to discover all other clients in the system.

## Distributed DataFrame and Array Layer

This layer provides abstractions like the distributed dataframe and arrays. 

### Distributed DataFrame and Column
A DataFrame represents multiple columns of data that each hold a specific type (`String`, `float`, `int`, `bool`). A column holds chunks that each hold actual values. A dataframe object does not actually hold the values directly; there are two layers of indirection needed when accessing a value inside a dataframe. 

A dataframe holds keys to its columns and a column holds keys to its chunks. When accessing a value inside a dataframe, there will be two key-value store lookups. The first lookup is getting the column from the key-value store using the column key and the second lookup is getting the chunk from the key-value store using the chunk key. Then there is a final access to get the value in the chunk.

## Application Layer

The user can operate on the dataframes. (Queries, Machine Learning, AI)


# Implementation

## Server
A `Server` class handles registraion of `Client` objects. It holds a directory of clients and can broadcast the registered clients to all registered clients.

## Client
A `Client` class holds connections to all other clients on the network. The client registers with the registration server on startup and discovers the other clients with the help of the server.

## Key
A `Key` object is used as key that is provided to a K/V store. A `Key` object contains a byte array and a node index where the value associated with this byte array key is stored.

## Value
A `Value` object is what a `Key` object is associated with. It holds a `char*` byte array of serialized data.

## KeyValueStore
The `KeyValueStore` class manages associations between `Key` objects and `Value` objects. A `KeyValueStore` on one process knows about `KeyValueStore` objects on other processes and can access `Value` objects on other processes. It is also possible to put `Value` objects into `KeyValueStore` objects on other processes. `KeyValueStore` objects each hold a `Client` object that is used for communication with other `KeyValueStore` objects.

Putting a key and value:
```cpp
void put(Key* k, Value* v)
```

Getting a value, returns `nullptr` if `Key` does not exist.
```cpp
Value* get(Key* k)
```

Getting a value, blocks/waits until `Key` exists if it does not exist.
```cpp
Value* getAndWait(Key* k)
```

## Chunk
A `Chunk` represents a subset of a `Column` Object. A `Chunk` always holds a constant number of values. The value type depends on the type of the `Column` object it is a subset of. 

## Column
A `Column` is a single column of a `DataFrame` Object. A `Column` can hold values of the following types:
- `String` object
- `float`
- `int`
- `bool`
A `Column` object holds `Key` objects that are associated with `Value` objects representing `Chunks` that hold the data in the column.

## DataFrame
The `DataFrame` class holds `Key` objects that are associated with `Value` objects representing `Column` objects that hold data. 

## Application
The `Application` class handles interactions with `DataFrame` objects using the `KeyValueStore`. The application is allowed to create `DataFrame` objects, store `DataFrame` objects in the `KeyValueStore` and retreive `DataFrame` objects from the `KeyValueStore`.

# Use cases
```cpp
class Demo : public Application {
public:
  Key main("main",0);
  Key verify("verif",0);
  Key check("ck",0);
 
  Demo(size_t idx): Application(idx) {}
 
  void run_() override {
    switch(this_node()) {
    case 0:   producer();     break;
    case 1:   counter();      break;
    case 2:   summarizer();
   }
  }
 
  void producer() {
    size_t SZ = 100*1000;
    double* vals = new double[SZ];
    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    DataFrame::fromArray(&main, &kv, SZ, vals);
    DataFrame::fromScalar(&check, &kv, sum);
  }
 
  void counter() {
    DataFrame* v = kv.waitAndGet(main);
    size_t sum = 0;
    for (size_t i = 0; i < 100*1000; ++i) sum += v->get_double(0,i);
    p("The sum is  ").pln(sum);
    DataFrame::fromScalar(&verify, &kv, sum);
  }
 
  void summarizer() {
    DataFrame* result = kv.waitAndGet(verify);
    DataFrame* expected = kv.waitAndGet(check);
    pln(expected->get_double(0,0)==result->get_double(0,0) ? "SUCCESS":"FAILURE");
  }
};
```

Use case of reading a SoR file into a dataFrame and parallel mapping a function over the dataFrame.

```cpp
SOR sorer("../data/data.sor");

DataFrame* df = sorer.read();

// Fibonacci is a subclass of Rower
Fibonacci fib(df);

df->map(fib);

delete df;
```


# Open questions
- What is the process for distributing the objects across multiple nodes? Whos responsibility is it to distribute chunks into different nodes?
- When accessing multiple values in the same chunk, the current strategy will require multiple iterations of deserializing the chunk and getting a single value when it is possible to deserialize the chunk once and cache the value. Is the implementation of a comprehensive caching stratgey/system required?
- Why is the key value store exposed to the application level? Should the user be allowed to pick which node to create a distributed DataFrame on (sample use case)?

# Status
What the team has:
- Ability to read SoR format and create a DataFrame from SoR files
- Ability to filter, map and parallel map over a DataFrame
- Ability to serialize and deserialize some objects
- Ability to send messages between clients and server
- Unit tests for existing components: DataFrames and Sorer
- Networking protocol
- Manually tested Networking code

What needs to be done:
- Ability to serialize and deserialize dataframes
- Create a Key/Value Store class
- Create an application class
- Create a Distributed DataFrame that can be stored across multiple nodes
