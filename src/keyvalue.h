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
        
        bool equals(Object* other) {
            Key* k = dynamic_cast<Key*>(other);
            if (k == nullptr) {
                return false;
            }
            return key_->equals(k->key_) && node_index_ == k->node_index_;
        }

        size_t hash() {
            return key_->hash() << 2 + node_index_;
        }

        Key* clone() {
            return new Key(node_index_, key_->c_str());
        }
};

// A value is a wrapper for a serialized object
class Value : public Object {
    public: 
        char* val_;  // owned
        size_t bytes_;

        Value(size_t bytes, const char* val) {
            bytes_ = bytes;
            val_ = new char[bytes];
            memcpy(val_, val, bytes);
        }

        Value(size_t bytes) {
            val_ = new char[bytes];
        }

        ~Value() {
            delete[] val_;
        }

        char* get() {
            return val_;
        }

        size_t size() {
            return bytes_;
        }

        bool equals(Object* o) {
            Value* other = dynamic_cast<Value*>(o);
            if (other == nullptr || other->bytes_ != bytes_) {
                return false;
            }

            for (size_t i = 0; i < bytes_; i++) {
                if (val_[i] != other->val_[i]) {
                    return false;
                }
            }
            return true;
        }

        size_t hash() {
            String s(val_);
            return s.hash() + bytes_;
        }

        Value* clone() {
            return new Value(bytes_, val_);
        }
};