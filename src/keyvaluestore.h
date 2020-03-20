//lang:Cpp
#pragma once

#include "object.h"
#include "string.h"
#include "network.h"
#include "map.h"
#include "keyvalue.h"

class KVStore : public Object {
    public:
        size_t node_index_;
        // map of local key -> values
        Map *map_;

        // network layer  -- TODO
        Client* client_;

        KVStore() {
            // create network
            // client_ = new Client(const char* server_ip, const char* client_ip, int client_listen_port, MessageHandler* msg_handler);

            node_index_ = 0; //client_->get_index();
            map_ = new Map();

        }

        ~KVStore() {
            // TODO: delete the keys and values owned by this key value store
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
                sleep(1);
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

        /*
        // we have to delete the Key from the map before we give them the value
        Value* remove(Key& key) {
            return map_->pop_item(&key);
        }
        */
};
