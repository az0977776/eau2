//lang:CwC
#pragma once

#include "../util/object.h"
#include "../dataframe/sorer.h"
#include "../dataframe/dataframe.h"
#include "../kvstore/keyvaluestore.h"

/**
 * This is the parent class to all applications that use a distributed key/value store.
 * This application can get, getAndWait, and put dataframes from a key/value store.
 * @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
 */
class Application : public Object{
    public:
        KVStore& kv;
        size_t node_idx_;
        Config config_;

        Application(KVStore& kvs) : kv(kvs), config_() {
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

        DataFrame* fromFile(const char* filename, Key* key, KVStore* kvs) {
            // sorer on filename
            SOR sorer(filename, key, kvs);

            // create df with key in kvs
            // return sorer.read(0, 1000 * 1000 * 1000);
            return sorer.read();
        }
        
        // what is the index of this node.
        virtual size_t this_node() {
            return node_idx_;
        }

        void set_output_color() {
            switch (this_node()) {
                case 0:
                    // red
                    printf("\033[0;31m");
                    break;
                case 1:
                    // green
                    printf("\033[0;32m");
                    break;
                case 2:
                    // blue
                    printf("\033[0;34m");
                    break;
                case 3:
                    // yellow
                    printf("\033[0;33m");
                    break;
                case 4:
                    // Magenta
                    printf("\033[0;35m");
                    break;
                case 5:
                    // Cyan
                    printf("\033[0;36m");
                    break;
                default:
                    // default color
                    reset_output_color();
                    break;
            }
        }

        void reset_output_color() {
            printf("\033[0m");
        }

        // this is a print function that will print out the node index of this application
        // used for debugging
        void print(const char* fmt, ...) {
            set_output_color();
            printf("NODE %zu: ", this_node());
            va_list ap;
            va_start(ap, fmt);
            vprintf(fmt, ap);
            va_end(ap);
            reset_output_color();
            fflush(stdout);
        }
};