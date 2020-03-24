//lang:Cpp
#pragma once

// #include "network.h"
#include "map.h"
#include "keyvalue.h"

#include "../util/object.h"
#include "../util/string.h"

class KVStore : public Object {
    public:
        size_t node_index_;
        // map of local key -> values
        Map *map_;

        // network layer  -- TODO
        // Client* client_;

        KVStore() {
            // create network
            // client_ = new Client(const char* server_ip, const char* client_ip, int client_listen_port, MessageHandler* msg_handler);

            node_index_ = 0; //client_->get_index();
            map_ = new Map();

        }

        ~KVStore() {
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
        }

        // gets a value from the kv store using a key, returns clone of value
        Value* get(Key& key) {
            Value* v = map_->get(&key);
            if ( v == nullptr ) {
                return nullptr;
            }
            return v->clone();
        }

        // gets a value from the kv store using a key, 
        // owned boolean is set to false if the value does not need to be deleted
        // owned boolean is set to true if the value needs to be deleted
        Value* get(Key& key, bool& owned) {
            owned = false;
            Value* v = map_->get(&key);
            return v;
        }

        Value* getAndWait(Key& key) {
            Value* val = nullptr;
            while ((val = map_->get(&key)) == nullptr) {
                // sleep(1);
            }
            return val->clone();
        }
        
        void put(Key& key, Value& value) {
            Value* temp = map_->get(&key);
            if (temp == nullptr) {
                // key does not exist in map
                map_->add(key.clone(), value.clone());
            } else if (!value.equals(temp)) {
                // key already exists so add a clone of the key, and delete the previous value
                map_->add(&key, value.clone());
                delete temp;
            }
        }
};
