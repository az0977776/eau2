//lang::CwC
#pragma once

#include "string.h"
#include "object.h"
#include "array.h"
#include "keyvalue.h"

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
        Array<Key> *keys_;
        Array<Value> *values_;
        // current number of buckets
        size_t num_buckets_;

        /* The constructor*/
        Map() { 
            num_buckets_ = 128;
            buckets_ = new Bucket*[num_buckets_];
            for (size_t i = 0; i < num_buckets_; i++) {
                buckets_[i] = new Bucket();
            }
            keys_ = new Array<Key>();
            values_ = new Array<Value>();
        }

        /* The destructor*/
        ~Map() { 
            for (size_t i = 0; i < num_buckets_; i++) {
                delete buckets_[i];
            }
            delete[] buckets_;
            delete keys_;
            delete values_;
        }

        /**
        * Determines the number of items in the map
        * @return the size of the map
        */
        size_t size() {
            return keys_->size();
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
                keys_->push(key);
                values_->push(value);
            } else {
                // key already exists so just replace the value
                // it is known that 0 <= keys_->indexOf(key) < size()
                values_->set(keys_->indexOf(key), value);
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
            keys_->clear();
            values_->clear();
        }

        /**
        * Returns a copy of the Map
        * @return the copy of this map
        */
        Map* copy(){
            Map *m0 = new Map();
            for (size_t i = 0; i < size(); i++) {
                Key* key = keys_->get(i);
                m0->add(key, get(key));
            }
            return m0;
        }

        /**
        * Returns the value of the specified key
        * @param key the key to get the value from
        * @return the value associated with the key
        */
        Value* get(Key* key) {
            size_t h = key->hash() % num_buckets_;
            return dynamic_cast<Value*>(buckets_[h]->get_val(key));
        }

        /**
        * Returns the Map's keys.
        */
        Key** keys() {
            return keys_->get_all();
        }

        /**
        * Returns all the Map's values
        */
        Value** values() {
            return values_->get_all();
        }

        /**
        * Removes the element with the specified key
        * @param key the key
        * @return the value of the element removed
        */
        Value* pop_item(Key* key) {
            size_t h = key->hash() % num_buckets_;
            Value *ret = dynamic_cast<Value*>(buckets_[h]->remove_kvpair(key));
            if (ret == nullptr) {
                return ret;
            }
            int idx = keys_->indexOf(key);
            if (idx < 0) {
                return nullptr;
            }
            keys_->remove(idx);
            values_->remove(idx);
            return ret;
        }

    /**
        * Is this object equal to that object?
        * @param o is the object to compare equality to.
        * @return	whether this object is equal to that object.
        */
        virtual bool equals(Object* o) {
            Map* other = dynamic_cast<Map*>(o);
            if (other == nullptr || other->size() != size()) {
                return false;
            }

            // check that all key value pairs are equal
            for (size_t i = 0; i < size(); i++) {
                Key *key = keys_->get(i);
                if (!get(key)->equals(other->get(key))) {
                    return false;
                }
            }
            return true;
        }

        /**
        * Calculate this object's hash.
        * @return a natural number of a hash for this object.
        */
        virtual size_t hash() {
            size_t ret = 0;
            Key** k = keys();
            Value** v = values();
            for (size_t i = 0; i <size(); i++) {
                ret = ret << 6;
                ret ^= k[i]->hash();
                ret ^= v[i]->hash();
            }
            delete[] k;
            delete[] v;
            return ret;
        }

        // rehashes the map
        // should happen when load factor (#elements/#buckets) > 0.75
        virtual void check_rehash_() {
            if (size() * 1.0 / num_buckets_ > 0.75) {
                // delete the current list of buckets
                for (size_t i = 0; i < num_buckets_; i++) {
                    delete buckets_[i];
                }
                delete[] buckets_;
                num_buckets_ *= 2;
                // recreate the list of buckets, but bigger
                buckets_ = new Bucket*[num_buckets_];
                for (size_t i = 0; i < num_buckets_; i++) {
                    buckets_[i] = new Bucket();
                }

                Array<Key>* keys_temp = keys_;
                Array<Value>* values_temp = values_;

                keys_ = new Array<Key>();
                values_ = new Array<Value>();

                // adds the keys and values into the new list
                for (size_t i = 0; i < keys_temp->size(); i++) {
                    add(keys_temp->get(i), values_temp->get(i));
                }

                delete keys_temp;
                delete values_temp;
            }
    }
};