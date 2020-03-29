//lang:CwC
#pragma once

#include "util/object.h"
#include "dataframe/dataframe.h"
#include "kvstore/keyvaluestore.h"

/**
 * This is the parent class to all applications that use a distributed key/value store.
 * This application can get, getAndWait, and put dataframes from a key/value store.
 * @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
 */
class Application : public Object{
    public:
        KVStore kv;
        size_t node_idx_;

        Application() : kv() {
            node_idx_ = kv.node_index();
        }

        ~Application() { }

        // To be overwritten by child classes. This is where the application code will be run from
        virtual void run_() { fail("Application run not implemented"); }
        
        // gets a dataframe with the given key value. If the key does not exist then return nullptr
        DataFrame* get(Key& key) {
            Value* v = kv.get(key);
            if (v == nullptr) {
                return nullptr;
            }
            return DataFrame::deserialize(v->get(), &kv);
        }

        // Try to get a dataframe with the given key. If the key does not exist, block until key does exist.
        DataFrame* getAndWait(Key& key) {
            Value* v = kv.getAndWait(key);
            abort_if_not(v != nullptr, "Application.getAndWait(): KVStore.getAndWait() returned a nullptr");
            return DataFrame::deserialize(v->get(), &kv);
        }

        // put the given dataframe into the key/value store with the given key.
        void put(Key& key, DataFrame& df) {
            char* buf = df.serialize();
            Value* v = new Value(df.serial_buf_size(), buf, true);
            kv.put(key, *v);

            delete v;
        }
        
        // what is the index of this node.
        virtual size_t this_node() {
            return node_idx_;
        }
};