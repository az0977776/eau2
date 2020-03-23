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

        Value* get(Key& key) {
            Value* v = map_->get(&key);
            if ( v == nullptr ) {
                return nullptr;
            }
            return v->clone();
        }

        Value* getAndWait(Key& key) {
            Value* val = nullptr;
            while ((val = map_->get(&key)) == nullptr) {
                // sleep(1);
            }
            return val->clone();
        }
        
        void put(Key& key, Value& value) {
            Value* temp = get(key);
            map_->add(key.clone(), value.clone());
            if (temp != nullptr) {
                delete temp;  // delete the old value
            }
        }
};
