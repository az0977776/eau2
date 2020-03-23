//lang::CwC
#pragma once

#include "keyvalue.h"

#include "../util/string.h"
#include "../util/object.h"
#include "../util/array.h"

// represents an array that is used for bucket in a hashmap
// each even index i is a key and i+1 is the value assocaited with the key
// does not own the objects passed into the array
class Bucket : public Array<Object> {
    public:
        Bucket() : Array() {}

        // checks if the underlying array needs to be reallocated
        // does so if true
        void check_reallocate_() {
            // since each object added to the array is a key then a value
            // check len_ + 1 to account for both objects added
            if (len_ + 1 >= cap_) {
                cap_*=2;
                Object **temp = new Object*[cap_];
                for (size_t i = 0; i < len_; i++) {
                    temp[i] = values_[i];
                }
                delete[] values_;
                values_ = temp;
            }
        }

        // removes a key value pair from the bucket array
        // takes in the key and returns the value
        // nullptr if o is not a valid key
        Object* remove_kvpair(Object *o) {
            size_t idx = len_;
            Object *val = nullptr;
            // find the key value pair to remove
            for (size_t i = 0; i < len_; i+=2) {
                if (o->equals(values_[i])) {
                    idx = i;
                    val = values_[i+1];
                    break;
                }
            }

            if (val == nullptr) {
                return nullptr;
            }

            // shift all remaining values left two
            for (size_t i = idx; i < len_ - 2; i++) {
                values_[i] = values_[i+2];
            }
            return val;
        }

        // adds a key value pair to the bucket array
        void add_kvpair(Object *k, Object *v) {
            int k_idx = indexOf(k);
            // if the key already exists in the bucket, overwrite the value
            if (k_idx >= 0) {
                values_[k_idx+1] = v;
            } else {
                // does not exist -> add it to the end
                check_reallocate_();
                values_[len_] = k;
                values_[len_+1] = v;
                len_+=2;
            }
        }

        // gets the value for the key in the bucket array
        // returns nullptr if not found
        Object* get_val(Object* k) {
            for (size_t i = 0; i < len_; i+=2) {
                if (k->equals(values_[i])) {
                    return values_[i+1];
                }
            }
            return nullptr;
        }
};

/**
* An object that represents a map to store keys and values.
* Map does not own any objects passed to it.
* @author barth.c@husky.neu.edu
*/
class Map : public Object {
    public:
        Bucket **buckets_;
        // the ith key in the keys_ array corresponds with the ith value in the values_ array
        // current number of buckets
        size_t num_buckets_;
        size_t size_;

        /* The constructor*/
        Map() { 
            num_buckets_ = 128;
            buckets_ = new Bucket*[num_buckets_];
            for (size_t i = 0; i < num_buckets_; i++) {
                buckets_[i] = new Bucket();
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
        void add(Key* key, Value* value) {
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
        * Removes all the elements from the Map
        */
        void clear() {
            for (size_t i = 0; i < num_buckets_; i++) {
                buckets_[i]->clear();
            }
        }

        /**
        * Returns the value of the specified key
        * @param key the key to get the value from
        * @return the value associated with the key
        */
        Value* get(Key* key) {
            size_t h = key->hash() % num_buckets_;
            // printf("h = %zu, num_buckets = %zu\n", h, num_buckets_);
            return dynamic_cast<Value*>(buckets_[h]->get_val(key));
        }

        /**
        * Removes the element with the specified key
        * @param key the key
        * @return the value of the element removed
        */
        Value* pop_item(Key* key) {
            size_t h = key->hash() % num_buckets_;
            Value *ret = dynamic_cast<Value*>(buckets_[h]->remove_kvpair(key));
            return ret;
        }

        Key** keys() {
            Bucket* bucket = nullptr;
            Key** ret = new Key*[size_];
            size_t counter = 0;
            for (size_t i = 0; i < num_buckets_; i++) {
                bucket = buckets_[i];
                for (size_t j = 0; j < bucket->size(); j+=2) {
                    ret[counter++] = dynamic_cast<Key*>(bucket->get(j));  // this is the key                      
                }
            }
            return ret;
        }

        Value** values() {
            Bucket* bucket = nullptr;
            Value** ret = new Value*[size_];
            size_t counter = 0;
            for (size_t i = 0; i < num_buckets_; i++) {
                bucket = buckets_[i];
                for (size_t j = 1; j < bucket->size(); j+=2) {
                    ret[counter++] = dynamic_cast<Value*>(bucket->get(j));  // this is the key                      
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
                num_buckets_ *= 2;
                
                Bucket** temp_buckets = new Bucket*[num_buckets_];
                for (size_t i = 0; i < num_buckets_; i++) {
                    temp_buckets[i] = new Bucket();
                }

                for (size_t i = 0; i < old_num_buckets; i++) {
                    for (size_t j = 0; j < buckets_[i]->size(); j+=2) {
                        Object* k = buckets_[i]->get(j);  // this is the key
                        Object* v = buckets_[i]->get(j+1);  // this is the value

                        h = k->hash() % num_buckets_;
                        temp_buckets[h]->add_kvpair(k, v);                        
                    }
                }

                // delete the current list of buckets
                for (size_t i = 0; i < old_num_buckets; i++) {
                    delete buckets_[i];
                }
                delete[] buckets_;
                
                buckets_ = temp_buckets;
            }
    }
};
