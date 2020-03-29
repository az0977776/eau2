//lang:Cpp
#pragma once

#include "network.h"
#include "map.h"
#include "keyvalue.h"

#include "../util/object.h"
#include "../util/string.h"
#include "../util/constant.h"

#include <pthread.h>

class KVStore;
class KVStoreMessageHandler : public MessageHandler {
    public:
        KVStore* kvs_; // not owned

        KVStoreMessageHandler(KVStore* kvs) {
            kvs_ = kvs;
        }
    
        ~KVStoreMessageHandler() { }

        void wait_for_node_index();
        Response* handle_message(sockaddr_in server, size_t data_len, char* data);
        Response* handle_get(sockaddr_in server, size_t data_len, char* data);
        Response* handle_get_and_wait(sockaddr_in server, size_t data_len, char* data);
        Response* handle_put(sockaddr_in server, size_t data_len, char* data);
};

class KVStore : public Object {
    public:
        bool server_; // is this KVStore connected to the server
        size_t node_index_;
        // map of local key -> values
        Map *map_;
        pthread_mutex_t lock_; // this locks the map of local values

        // Key* cached_keys_[KVSTORE_CACHE_SIZE];  // key.hash() % KVSTORE_CACHE_SIZE = index, key.equals(array[index]) 
        // Value* cached_values_[KVSTORE_CACHE_SIZE];

        // network layer
        Client* client_;

        KVStore() : KVStore(true) { }

        KVStore(bool server) {
            abort_if_not(pthread_mutex_init(&lock_, NULL) == 0, "KVStore: Failed to create mutex");
            map_ = new Map();
            server_ = server;
            
            if (server_) {
                // this is to have a value to compare to and check that it has been set before continuing
                // this is checked in KVStoreMessageHandler.handle_get_and_wait
                node_index_ = -1; 
                // create network
                client_ = new Client(SERVER_IP, CLIENT_IP, new KVStoreMessageHandler(this));
                node_index_ = client_->get_index();
            } else {
                client_ = nullptr;
                node_index_ = 0;
            }
        }

        ~KVStore() {
            lock_map();
            Key** keys = map_->keys();
            Value** vals = map_->values();
            size_t size = map_->size();

            for (size_t i = 0; i < size; i++) {
                delete keys[i];
                delete vals[i];
            }

            delete[] keys;
            delete[] vals;
            delete map_;

            if (server_) {  
                delete client_->get_message_handler();
                delete client_; // will wait for client listening thread
            }

            unlock_map();
            // destroy the lock
            pthread_mutex_destroy(&lock_); 
        }

        void lock_map() {
            pthread_mutex_lock(&lock_);
        }

        void unlock_map() {
            pthread_mutex_unlock(&lock_);
        }

        size_t node_index() {
            return node_index_;
        }

        sockaddr_in get_sender() {
            if (server_) {
                return client_->get_sockaddr();
            } else {
                return { 0 }; 
            }
        }

        // gets a value from the kv store using a key, returns clone of value
        Value* get(Key& key) {
            bool owned = false;
            Value* v = get(key, owned);
            if ( v == nullptr ) {
                return nullptr;
            }
            if (owned) {
                return v;
            } else {
                return v->clone();
            }
        }

        // gets a value from the kv store using a key, 
        // owned boolean is set to false if the value does not need to be deleted
        // owned boolean is set to true if the value needs to be deleted
        Value* get(Key& key, bool& owned) {
            Value* ret = nullptr;
            if (key.get_index() == node_index_) {
                owned = false;
                lock_map();
                ret = map_->get(&key);
                unlock_map();
            } else if (server_) {
                owned = true;

                char* key_buf = key.serialize();
                size_t return_len = 0;
                char* returned_value = client_->get(key.get_index(), key.serial_buf_size(), key_buf, return_len);
                
                if (returned_value != nullptr) {
                    ret = new Value(return_len, returned_value, true); // stealing the char* returned_value
                }
                
                delete[] key_buf;
            } else {
                fail("KVStore.get(): Got a key to a different node while client was not running");
            }
            return ret;
        }

        Value* getAndWait(Key& key) {
            bool owned = false;
            Value* v = getAndWait(key, owned);
            if ( v == nullptr ) {
                return nullptr;
            }
            if (owned) {
                return v;
            } else {
                return v->clone();
            }
        }

        Value* getAndWait(Key& key, bool &owned) {
            if (key.get_index() == node_index_) {
                owned = false;
                Value* val = nullptr;
                while (true) {
                    lock_map();
                    if ((val = map_->get(&key)) != nullptr) {
                        unlock_map();
                        break;
                    }
                    unlock_map();
                    sleep(1);
                }
                return val;
            } else if (server_) {
                owned = true;
                char* key_buf = key.serialize();
                size_t return_len = 0;
                char* returned_value = client_->get_and_wait(key.get_index(), key.serial_buf_size(), key_buf, return_len);

                Value* v = new Value(return_len, returned_value, true); // stealing the char* returned_value
                
                delete[] key_buf;
                return v;
            } else {
                fail("KVStore.getAndWait(): Got a key to a different node while client was not running");
                return nullptr;
            }
        }
        
        void put(Key& key, Value& value) {
            if (key.get_index() == node_index_) {
                lock_map();
                Value* temp = map_->get(&key);
                if (temp == nullptr) {
                    // key does not exist in map
                    map_->add(key.clone(), value.clone());
                } else if (!value.equals(temp)) {
                    // key already exists so add a clone of the key, and delete the previous value
                    map_->add(&key, value.clone());
                    delete temp;
                }
                unlock_map();
            } else if (server_) {
                size_t buf_len = key.serial_buf_size() + value.size();
                char* buf = new char[buf_len];
                key.serialize(buf);
                memcpy(buf + key.serial_buf_size(), value.get(), value.size());

                client_->put(key.get_index(), buf_len, buf);
                delete[] buf;
            } else {
                fail("KVStore.put(): Got a key to a different node while client was not running");
            }
        }
};


// KVStoreMessageHandler methods

void KVStoreMessageHandler::wait_for_node_index() {
    while (kvs_->node_index() == MAX_SIZE_T) {
        sleep(1);
    } 
}

// handle a generic message coming from the given sender
// return: the response the the given message.
//          if respnse is nullptr then nothing is sent back.
Response* KVStoreMessageHandler::handle_message(sockaddr_in server, size_t data_len, char* data) { 
    fail("KVStore got a undefined message");
    return nullptr;
}

// handle a generic get coming from the given sender
// return: the response the the given message.
//          if respnse is nullptr then nothing is sent back.
Response* KVStoreMessageHandler::handle_get(sockaddr_in server, size_t data_len, char* data) {
    bool owned = false;

    Key* key = Key::deserialize(data);
    wait_for_node_index();
    abort_if_not(key->get_index() == kvs_->node_index(), "KVStore got a GET request for the wrong node");

    Value* v = kvs_->get(*key, owned);
    abort_if_not(!owned, "KVStore.handle_get(): Should not own the value");

    if (v == nullptr) {
        return nullptr;
    }
    return new Response(kvs_->get_sender(), v->size(), v->get());
}

// handle a generic get and wait coming from the given sender
// return: the response the the given message.
//          if respnse is nullptr then nothing is sent back.
Response* KVStoreMessageHandler::handle_get_and_wait(sockaddr_in server, size_t data_len, char* data) {
    bool owned = false;

    Key* key = Key::deserialize(data);
    wait_for_node_index();
    abort_if_not(key->get_index() == kvs_->node_index(), "KVStore on node %zu got a GET_AND_WAIT request for node %zu", kvs_->node_index(), key->get_index());

    Value* v = kvs_->getAndWait(*key, owned);
    abort_if_not(!owned, "KVStore.handle_get_and_wait(): Should not own the value");

    Response* rv = new Response(kvs_->get_sender(), v->size(), v->get());
    return rv;
}

// handle a generic put coming from the given sender
// return: the response the the given message.
//          if respnse is nullptr then nothing is sent back.
Response* KVStoreMessageHandler::handle_put(sockaddr_in server, size_t data_len, char* data) {
    // print_byte(data, data_len);

    Key* key = Key::deserialize(data);
    wait_for_node_index();
    abort_if_not(key->get_index() == kvs_->node_index(), "KVStore got a PUT request for the wrong node");

    Value* val = new Value(data_len - key->serial_buf_size(), data + key->serial_buf_size());

    kvs_->put(*key, *val);
    delete key;
    delete val;

    return nullptr;
}

/*
g++ -pthread -O3 -Wall -pedantic -std=c++11 tests/milestone_3.cpp -o milestone3
./milestone3 &
./milestone3 &
./milestone3
Current node idx: 1
This is the counter node with node idx: 1
Exit message: "KVStore on node 4570972160 got a GET_AND_WAIT request for node 0 -- errno: 0
Exit message: "send_message().check_header2: Did not read the correct numnber of bytes -- errno: 42
Exit message: "send_message().check_header2: Did not read the correct numnber of bytes -- errno: 42
make: *** [milestone3] Error 255
➜  eau2 git:(master) ✗ make milestone3
g++ -pthread -O3 -Wall -pedantic -std=c++11 tests/milestone_3.cpp -o milestone3
./milestone3 &
./milestone3 &
./milestone3
Current node idx: 1
This is the counter node with node idx: 1
Exit message: "KVStore on node 4653596672 got a GET_AND_WAIT request for node 0 -- errno: 0
Exit message: "send_message().check_header2: Did not read the correct numnber of bytes -- errno: 42
make: *** [milestone3] Error 255
Exit message: "send_message().check_header2: Did not read the correct numnber of bytes -- errno: 42



*/