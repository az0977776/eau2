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
/**
 * This is a message handler for the Key Value store. It can handle get and put requests
 * to the Key value store. 
 * @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
 */
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

/**
 * This is a mapping between Key and Value objects. This Key Value store can access other
 * KVStores that are on the same network.
 * @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
 */
class KVStore : public Object {
    public:
        // is this KVStore connected to the server? (This is used to test without a network)
        bool server_; 
        
        // map of local Key -> Value
        Map map_;
        pthread_mutex_t lock_; // this locks the map of local values
        
        size_t node_index_;

        // network layer
        Client* client_;

        KVStore() : KVStore(true) { }

        KVStore(bool server) : map_() {
            abort_if_not(pthread_mutex_init(&lock_, NULL) == 0, "KVStore: Failed to create mutex");
            server_ = server;
            
            if (server_) {
                // this is to have a value to compare to and check that it has been set before continuing
                // this is checked in KVStoreMessageHandler
                node_index_ = -1; 

                // create network
                client_ = new Client(SERVER_IP, CLIENT_IP, new KVStoreMessageHandler(this));
                node_index_ = client_->get_index();
            } else {
                // no server is running so don't start a client
                client_ = nullptr;
                node_index_ = 0;
            }
        }

        ~KVStore() {
            lock_map();
            Key** keys = map_.keys();
            Value** vals = map_.values();
            size_t size = map_.size();

            for (size_t i = 0; i < size; i++) {
                delete keys[i];
                delete vals[i];
            }

            delete[] keys;
            delete[] vals;

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

        // returns the sockaddr_in associated with the client if it exists, else return an empty sockaddr_in
        sockaddr_in get_sender() {
            if (server_) {
                return client_->get_sockaddr();
            } else {
                return { 0 }; 
            }
        }

        // gets a value from the kv store using a key, returns clone of value
        // returned value is owned by caller
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
        // owned boolean is set to false if the value returned does not need to be deleted
        // owned boolean is set to true if the value returned needs to be deleted
        Value* get(Key& key, bool& owned) {
            Value* ret = nullptr;
            // if the value is stored in the local kvstore
            if (key.get_index() == node_index_) {
                owned = false;
                lock_map();
                ret = map_.get(&key);
                unlock_map();
            // if the value is not stored in the local kvstore
            } else if (server_) {
                owned = true;

                char* key_buf = key.serialize();
                size_t return_len = 0;
                
                // gets the value using the network client
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

        // waits for a key to appear in the kvstore and returns the value associated with the key when it appears
        // returned value is owned by caller
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

        // waits for a key to appear in the kvstore and returns the value associated with the key when it appears
        // owned boolean is set to false if the value returned does not need to be deleted
        // owned boolean is set to true if the value returned needs to be deleted
        Value* getAndWait(Key& key, bool &owned) {
            Value* val = nullptr;
            // if the value is stored in the local kvstore
            if (key.get_index() == node_index_) {
                owned = false;
                while (true) {
                    lock_map();
                    if ((val = map_.get(&key)) != nullptr) {
                        unlock_map();
                        break;
                    }
                    unlock_map();
                    sleep(1);
                }
            // if the value is not stored in the local kvstore
            } else if (server_) {
                owned = true;
                char* key_buf = key.serialize();
                size_t return_len = 0;
                
                // gets the value using the network client
                char* returned_value = client_->get_and_wait(key.get_index(), key.serial_buf_size(), key_buf, return_len);

                val = new Value(return_len, returned_value, true); // stealing the char* returned_value
                
                delete[] key_buf;
            } else {
                fail("KVStore.getAndWait(): Got a key to a different node while client was not running");
            }
            return val;
        }
        
        // adds a key value pair to the kv store
        void put(Key& key, Value& value) {
            // if adding to the local kvstore
            if (key.get_index() == node_index_) {
                lock_map();
                Value* temp = map_.get(&key);
                if (temp == nullptr) {
                    // key does not exist in map
                    map_.add(key.clone(), value.clone());
                } else if (!value.equals(temp)) {
                    // key already exists so add a clone of the key, and delete the previous value
                    map_.add(&key, value.clone());
                    delete temp;
                }
                unlock_map();
            // if not adding to the local kvstore
            } else if (server_) {
                size_t buf_len = key.serial_buf_size() + value.size();
                char* buf = new char[buf_len];
                key.serialize(buf);
                memcpy(buf + key.serial_buf_size(), value.get(), value.size());

                // adding the key value pair to another node using the network client
                client_->put(key.get_index(), buf_len, buf);
                delete[] buf;
            } else {
                fail("KVStore.put(): Got a key to a different node while client was not running");
            }
        }
};


// KVStoreMessageHandler methods

// Make sure that the node index of KVStore is set before continuing. The node index is MAZ_SIZE_T
// before it is set to the correct value
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
// return: the response the the given message. if respnse is nullptr then nothing is sent back.
// @note: Put should have no return message
Response* KVStoreMessageHandler::handle_put(sockaddr_in server, size_t data_len, char* data) {
    Key* key = Key::deserialize(data);
    wait_for_node_index();
    abort_if_not(key->get_index() == kvs_->node_index(), "KVStore got a PUT request for the wrong node");

    Value* val = new Value(data_len - key->serial_buf_size(), data + key->serial_buf_size());

    kvs_->put(*key, *val);
    delete key;
    delete val;

    return nullptr;
}