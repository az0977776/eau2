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
        Map<Key, Value> *kv;

        // network layer  -- TODO
        Client* client_;

        KVStore() {
            // create network
            // client_ = new Client(const char* server_ip, const char* client_ip, int client_listen_port, MessageHandler* msg_handler);

            node_index_ = 0; //client_->get_index();
            kv = new Map<Key, Value>();

        }

        Value* get(Key& key) {
            return kv->get(&key);
        }

        Value* getAndWait(Key& key) {
            Value* val = nullptr;
            while ((val = kv->get(&key)) == nullptr) {
                sleep(1);
            }
            return val;
        }
        
        void put(Key& key, Value& value) {
            kv->add(&key, &value);
        }
};