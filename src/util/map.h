//lang::CwC
#pragma once

#include "string.h"
#include "object.h"
#include "array.h"

// represents an array that is used for bucket in a hashmap
// each even index i is a key and i+1 is the value assocaited with the key
// does not own the objects passed into the array
// @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
template <class K, class V>
class Bucket : public Object {
    public:
        Array<K>* keys_;
        Array<V>* values_;
        
        Bucket() {
            keys_ = new Array<K>();
            values_ = new Array<V>();
        }

        ~Bucket() {
            delete keys_;
            delete values_;
        }

        // removes a key value pair from the bucket array
        // takes in the key and returns the value
        // nullptr if o is not a valid key
        V* remove_kvpair(K* key) {
            int idx = keys_->indexOf(key);
            if (idx < 0) {
                return nullptr;
            }

            keys_->remove(idx);
            return values_->remove(idx);
        }

        // adds a key value pair to the bucket array
        void add_kvpair(K* k, V* v) {
            int k_idx = keys_->indexOf(k);
            // if the key already exists in the bucket, overwrite the value
            if (k_idx >= 0) {
                // does not delete the value it replaces
                values_->set(k_idx, v);
            } else {
                // does not exist -> add it to the end
                keys_->push_back(k);
                values_->push_back(v);
            }
        }

        // gets the value for the key in the bucket array
        // returns nullptr if not found
        V* get_val(K* k) {
            int idx = keys_->indexOf(k);
            return idx < 0 ? nullptr : values_->get(idx);
        }

        V* get_val(size_t idx) {
            return values_->get(idx);
        }

        K* get_key(size_t idx) {
            return keys_->get(idx);
        }

        size_t size() {
            abort_if_not(keys_->size() == values_->size(), "Bucket key array size does not match value array size");
            return keys_->size();
        }
};

/**
* An object that represents a map to store keys and values.
* Map does not own any objects passed to it.
* @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
*/
template<class K, class V>
class Map : public Object {
    public:
        Bucket<K, V> **buckets_;
        // the ith key in the keys_ array corresponds with the ith value in the values_ array
        // current number of buckets
        size_t num_buckets_;
        size_t size_;

        /* The constructor*/
        Map() { 
            num_buckets_ = 1024;
            buckets_ = new Bucket<K,V>*[num_buckets_];
            for (size_t i = 0; i < num_buckets_; i++) {
                buckets_[i] = new Bucket<K, V>();
            }
            size_ = 0;
        }

        /* The destructor*/
        ~Map() { 
            for (size_t i = 0; i < num_buckets_; i++) {
                delete buckets_[i];
            }
            delete[] buckets_;
        }

        /**
        * Determines the number of items in the map
        * @return the size of the map
        */
        size_t size() {
            return size_;
        }

        /**
        * Adds the given key value pair to the Map
        * @param key is the object to map the value to
        * @param value the object to add to the Map
        */
        void add(K* key, V* value) {
            // if the key does not exist in the bucket, add key and value to their array
            size_t h = key->hash() % num_buckets_;
            Object* o = buckets_[h]->get_val(key);
            if (o == nullptr) {
                // key does not exist yet
                check_rehash_();
                h = key->hash() % num_buckets_;
                size_++;
            } 
            buckets_[h]->add_kvpair(key, value);
        }

        /**
        * Returns the value of the specified key
        * @param key the key to get the value from
        * @return the value associated with the key
        */
        V* get(K* key) {
            size_t h = key->hash() % num_buckets_;
            return buckets_[h]->get_val(key);
        }

        /**
        * Removes the element with the specified key
        * @param key the key
        * @return the value of the element removed
        */
        V* pop_item(K* key) {
            size_t h = key->hash() % num_buckets_;
            V* ret = buckets_[h]->remove_kvpair(key);
            return ret;
        }

        K** keys() {
            Bucket<K,V>* bucket = nullptr;
            K** ret = new K*[size_];
            size_t counter = 0;
            for (size_t i = 0; i < num_buckets_; i++) {
                bucket = buckets_[i];
                for (size_t j = 0; j < bucket->size(); j++) {
                    ret[counter++] = bucket->get_key(j);                      
                }
            }
            return ret;
        }

        V** values() {
            Bucket<K,V>* bucket = nullptr;
            V** ret = new V*[size_];
            size_t counter = 0;
            for (size_t i = 0; i < num_buckets_; i++) {
                bucket = buckets_[i];
                for (size_t j = 0; j < bucket->size(); j++) {
                    ret[counter++] = bucket->get_val(j);  // this is the key                      
                }
            }
            return ret;
        }

        // rehashes the map
        // should happen when load factor (#elements/#buckets) > 0.75
        virtual void check_rehash_() {
            if (size() * 1.0 / num_buckets_ > 0.75) {
                size_t h = 0;
                size_t old_num_buckets = num_buckets_;
                size_t new_num_buckets = 2 * num_buckets_;
                
                Bucket<K,V>** new_buckets = new Bucket<K,V>*[new_num_buckets];
                for (size_t i = 0; i < new_num_buckets; i++) {
                    new_buckets[i] = new Bucket<K, V>();
                }

                for (size_t i = 0; i < old_num_buckets; i++) {
                    for (size_t j = 0; j < buckets_[i]->size(); j++) {
                        K* k = buckets_[i]->get_key(j); 
                        V* v = buckets_[i]->get_val(j); 

                        h = k->hash() % new_num_buckets;
                        new_buckets[h]->add_kvpair(k, v);                        
                    }
                }

                // delete the current list of buckets
                for (size_t i = 0; i < old_num_buckets; i++) {
                    delete buckets_[i];
                }
                delete[] buckets_;
                
                buckets_ = new_buckets;
                num_buckets_ = new_num_buckets;
            }
    }
};
