//lang::CwC
#pragma once

#include "object.h"
#include "string.h"

// A key is associates a String with a node index where the data is located
class Key : public Object {
    public:
        size_t node_index_;
        String* key_; // owned

        Key(size_t node_index, const char* key) {
            node_index_ = node_index;
            key_ = new String(key);
        }

        ~Key() {
            delete key_;
        }
        
        bool equals(Key* k) {
            return key_->equals(k) && node_index_ == k->node_index_;
        }
};

// A value is a wrapper for a serialized object
class Value : public Object {
    public: 
        char* val_;  // owned

        Value(size_t bytes, const char* val) {
            val_ = new char[bytes];
            memcpy(val_, val, bytes);
        }

        ~Value() {
            delete[] val_;
        }

        char* get() {
            return val_;
        }
};