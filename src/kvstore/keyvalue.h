//lang::CwC
#pragma once

#include "../util/object.h"
#include "../util/string.h"

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

        String* get_name() {
            return key_->clone();
        }

        size_t get_index() {
            return node_index_;
        }
        
        bool equals(Object* other) {
            Key* k = dynamic_cast<Key*>(other);
            if (k == nullptr) {
                return false;
            }
            return key_->equals(k->key_) && node_index_ == k->node_index_;
        }

        size_t hash() {
            return (key_->hash() << 2) + node_index_;
        }

        Key* clone() {
            return new Key(node_index_, key_->c_str());
        }

        void print() {
            pln(key_->c_str());
        }

        size_t serial_buf_size() {
            return sizeof(size_t) + key_->size() + 1;
        }

        char* serialize(char* buf) {
            memcpy(buf, &node_index_, sizeof(size_t));
            memcpy(buf + sizeof(size_t), key_->c_str(), key_->size() + 1); 
            return buf;
        }

        char* serialize() {
            char* buf = new char[serial_buf_size()];
            return serialize(buf);
        }

        static Key* deserialize(const char* buf) {
            size_t node_index = 0;
            memcpy(&node_index, buf, sizeof(size_t));

            size_t name_len = strlen(buf + sizeof(size_t)) + 1;
            char* name = new char[name_len];
            memcpy(name, buf + sizeof(size_t), name_len); 

            Key* ret = new Key(node_index, name);
            delete[] name;
            return ret;
        }
};

// A value is a wrapper for a serialized object
class Value : public Object {
    public: 
        char* val_;  // owned
        size_t bytes_;

        Value(size_t bytes, char* val, bool steal) {
            bytes_ = bytes;
            if (steal) {
                val_ = val;
            } else {
                val_ = new char[bytes];
                memcpy(val_, val, bytes);
            }
        }

        Value(size_t bytes, char* val) : Value(bytes, val, false) { }

        Value(size_t bytes) : Value(bytes, new char[bytes], true) {
            memset(val_, 0, bytes); // to fix valgrind error
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

        // For debugging the bytes. this will print the val in four size_t as hexs
        void print() {
            size_t* temp = reinterpret_cast<size_t*>(val_);
            size_t len = bytes_ / sizeof(size_t);
            for (size_t i = 0; i < len; i += 4) {
                printf("[bytes %zu to %zu]  0x%zX   0x%zX   0x%zX   0x%zX\n", i * sizeof(size_t), (i + 4) * sizeof(size_t) - 1, temp[i], temp[i + 1], temp[i + 2], temp[i + 3]);
            }
        }

        // for debugging the bytes as strings
        // prints until a null byte, then new lines
        void print_strings() {
            printf("PRINTING VALUE AS STRINGS\n");
            char* to_print = val_;
            size_t string_count = 0;
            while ((size_t)(to_print - val_) < bytes_) {
                printf("String #%zu: len = %zu \"%s\"\n", string_count, strlen(to_print), to_print);
                
                to_print += (strlen(val_) + 1);
                string_count++;
            }
        }
};