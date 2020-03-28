//lang:CwC
#pragma once

#include "util/object.h"
#include "dataframe/dataframe.h"
#include "kvstore/keyvaluestore.h"

class Application : public Object{
    public:
        KVStore kv;
        size_t node_idx_;

        Application() : kv() {
            node_idx_ = kv.node_index();
        }

        ~Application() { }

        virtual void run_() { fail("Application run not implemented"); }
        
        DataFrame* get(Key& key) {
            Value* v = kv.get(key);
            if (v == nullptr) {
                return nullptr;
            }
            return DataFrame::deserialize(v->get(), &kv);
        }

        DataFrame* getAndWait(Key& key) {
            Value* v = kv.getAndWait(key);
            abort_if_not(v != nullptr, "Application.getAndWait(): KVStore.getAndWait() returned a nullptr");
            return DataFrame::deserialize(v->get(), &kv);
        }

        void put(Key& key, DataFrame& df) {
            char* buf = df.serialize();
            Value* v = new Value(df.serial_buf_size(), buf, true);
            kv.put(key, *v);

            delete v;
        }
        
        virtual size_t this_node() {
            return node_idx_;
        }
};