//lang:Cpp
#pragma once

#include <stdarg.h>
#include <stdlib.h>

#include "object.h"
#include "string.h"
#include "keyvalue.h"
#include "keyvaluestore.h"
#include "vector.h"

static const size_t ARRAY_STARTING_CAP = 4;

enum ColumnType {
    UNKNOWN = 0,
    BOOL = 'B', 
    INT = 'I', 
    DOUBLE = 'D',
    STRING = 'S'
};

size_t column_type_to_num(char t) {
    switch (t) {
        case UNKNOWN:
            return 0;
        case BOOL:
            return 1;
        case INT:
            return 2;
        case DOUBLE:
            return 3;
        case STRING:
            return 4;
        default:
            abort();
    }
}

bool is_int(char *c) {
    if (*c == '\0') {
        return false;
    }
    for (int i = 0; c[i] != '\0'; i++) {
        if (i == 0 && (c[i] == '+' || c[i] == '-')) {
            continue;
        } else if (!isdigit(c[i])) {
            return false;
        }
    }
    return true;
}

bool is_double(char *c) {
    if (*c == '\0') {
        return false;
    }
    bool has_decimal = false;
    for (int i = 0; c[i] != '\0'; i++) {
        if (i == 0 && (c[i] == '+' || c[i] == '-')) {
            continue;
        } else if (c[i] == '.' && has_decimal) {
            return false;
        } else if (c[i] == '.') {
            has_decimal = true;
        } else if (!isdigit(c[i])) {
            return false;
        }
    }
    return true;
}

bool as_bool(char* c) {
    if (*c == '1') {
        return true;
    } else {
        return false;
    }
}

int as_int(char* c) {
    return atoi(c);
}

double as_double(char* c) {
    return atof(c);
}

String* as_string(char* c) {
    return new String(c);
}


// returns the inferred typing of the char*
char infer_type(char *c) {
    // missing values
    if (c == nullptr) {
        return BOOL;
    }
    // check boolean
    if (strlen(c) == 1) {
        if ((*c == '0') || (*c == '1')) {
            return BOOL;
        }
    }
    // check int
    if (is_int(c)) {
        return INT;
    }
    // check double
    if (is_double(c)) {
        return DOUBLE;
    }
    return STRING;
}

class StringColumn;
class DoubleColumn;
class IntColumn;
class BoolColumn;

static const size_t CHUNK_SIZE = sizeof(size_t) * 128;  // on 64-bit machine this is 8 * 128 = 1024

/**************************************************************************
 * Column ::
 * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Columns are mutable, equality is pointer
 * equality. */
class Column : public Object {
    public:
        KVStore* kv_;  // external
        String* col_name_;  // owned
        Vector<Key*> *chunk_keys_;

        size_t len_;

        Column(String* col_name, KVStore* kv) {
            len_ = 0;
            kv_ = kv;
            col_name_ = col_name->clone();
            chunk_keys_ = new Vector<Key*>();
        }

        // NOTE: takes ownership of chunk_keys and the keys inside the vector
        Column(size_t len, Vector<Key*>* chunk_keys, String* col_name, KVStore* kv) {
            len_ = len;
            kv_ = kv;
            col_name_ = col_name->clone();
            chunk_keys_ = chunk_keys;
        }

        ~Column() {
            delete col_name_;
            for (size_t i = 0; i < len_; i++) {
                delete chunk_keys_->get(i);
            }
            delete chunk_keys_;
        }

        char* generate_chunk_name(size_t chunk_idx) {
            size_t col_name_len = col_name_->size();
            char* ret = new char[col_name_len + 3 + (sizeof(size_t) * 2)];
            memcpy(ret, col_name_->c_str(), col_name_len);
            ret[col_name_len] = ':';
            sprintf(ret+col_name_len+1, "0x%X", (unsigned int)chunk_idx);
            return ret;
        }

        /** Type converters: Return same column under its actual type, or
         *  nullptr if of the wrong type.  */
        virtual IntColumn* as_int() {
            abort_if_not(false, "Column.as_int(): bad conversion");
            return nullptr;
        }

        virtual BoolColumn*  as_bool() {
            abort_if_not(false, "Column.as_bool(): bad conversion");
            return nullptr;
        }
        
        virtual DoubleColumn* as_double() {
            abort_if_not(false, "Column.as_double(): bad conversion");
            return nullptr;
        }

        virtual StringColumn* as_string() {
            abort_if_not(false, "Column.as_string(): bad conversion");
            return nullptr;
        }

        /** Type appropriate push_back methods. Calling the wrong method is
            * undefined behavior. **/
        virtual void push_back(int val) {
            abort_if_not(false, "Column.push_back(int): push_back bad value");
        }
        virtual void push_back(bool val) {
            abort_if_not(false, "Column.push_back(bool): push_back bad value");
        }
        virtual void push_back(double val) {
            abort_if_not(false, "Column.push_back(double): push_back bad value");
        }
        virtual void push_back(String* val) {
            abort_if_not(false, "Column.push_back(String*): push_back bad value");
        }

        /** Returns the number of elements in the column. */
        virtual size_t size() {
            return len_;
        }

        /** Return the type of this column as a char: 'S', 'B', 'I' and 'D'. */
        char get_type() {
            return get_type_();
        }

        // helper function for get_type, is protected, should be overwritten in subclasses
        virtual char get_type_() {
            return 0;
        }

        size_t serial_buf_size() {
            size_t ret = 1 + sizeof(size_t) + col_name_->size() + 1; // char for type and size_t for length and the name of column
            for (size_t i = 0; i < chunk_keys_->size(); i++) {
                ret += chunk_keys_->get(i)->serial_buf_size();
            }
            return ret;
        }

        // <type><len_><name>[key...] 
        char* serialize(char* buf) {
            char* buf_pointer = buf;
            buf_pointer[0] = get_type();
            buf_pointer += 1;

            memcpy(buf_pointer, &len_, sizeof(size_t));
            buf_pointer += sizeof(size_t);

            memcpy(buf_pointer, col_name_->c_str(), col_name_->size() + 1);
            buf_pointer += col_name_->size() + 1;
            
            for (size_t i = 0; i < chunk_keys_->size(); i++) {
                chunk_keys_->get(i)->serialize(buf_pointer);
                buf_pointer += chunk_keys_->get(i)->serial_buf_size();
            }
            return buf;
        }

        // <type><len_><name>[key...] 
        char* serialize() {
            char* buf = new char[serial_buf_size()];
            return serialize(buf);
        }

        static Column* deserialize(const char* buf, KVStore* kvs);
};
  

/*************************************************************************
 * BoolColumn::
 * Holds bool values.
 */
class BoolColumn : public Column {
    public:       
        BoolColumn(String* col_name, KVStore* kv) : Column(col_name, kv) { }

        BoolColumn(String* col_name, KVStore* kv, int n, ...) : Column(col_name, kv) {
            va_list arguments;
            va_start (arguments, n);
            for (int i = 0; i <  n; i++ ) {
                bool b = va_arg(arguments, int);
                push_back(b);
            }
            va_end(arguments);
        } 
        
        // NOTE: takes ownership of chunk_keys and the keys inside the vector
        BoolColumn(size_t len, Vector<Key*>* chunk_keys, String* col_name, KVStore* kv) : Column(len, chunk_keys, col_name, kv) { }

        ~BoolColumn() { }

        void check_and_reallocate_() {
            // when the latest chunk is full
            if (len_ % CHUNK_SIZE == 0) {
                size_t chunk_idx = len_ / CHUNK_SIZE;

                char* new_chunk_key = generate_chunk_name(chunk_idx);

                Key* k = new Key(0, new_chunk_key);
                Value v(CHUNK_SIZE);
                v.set_zero();
                kv_->put(*k, v);

                chunk_keys_->push_back(k);
            }
        }

        virtual void push_back(bool val) {
            check_and_reallocate_();
            size_t chunk_idx = len_ / CHUNK_SIZE;
            size_t item_idx = len_ / (CHUNK_SIZE / sizeof(size_t));
            Key* chunk_key = chunk_keys_->get(chunk_idx);

            Value* value = kv_->get(*chunk_key);
            char* v = value->get();

            size_t buf = 0;
            memcpy(&buf, v + item_idx * sizeof(size_t), sizeof(size_t));
            size_t bit_idx = len_ % (sizeof(size_t) * 8);

            if (val) {
                buf |= (1 << bit_idx);
            } else {
                buf &= (~(1 << bit_idx));
            }

            memcpy(v + item_idx * sizeof(size_t), &buf, sizeof(size_t));
            Value new_value(CHUNK_SIZE, v);
            kv_->put(*chunk_key, new_value);

            len_++;
            delete value;
        }
        
        // gets the bool at the index idx
        // if idx is out of bounds, exit
        bool get(size_t idx) {
            abort_if_not(idx < size(), "BoolColumn.get(): out of bounds");
            size_t chunk_idx = idx / CHUNK_SIZE;
            size_t item_idx = idx / (CHUNK_SIZE / sizeof(size_t));
            Key* chunk_key = chunk_keys_->get(chunk_idx);

            Value* value = kv_->get(*chunk_key);
            char* v = value->get();
            size_t buf;
            memcpy(&buf, v + item_idx * sizeof(size_t), sizeof(size_t));
            size_t bit_idx = idx % (sizeof(size_t) * 8); // number of bits in size_t

            bool ret = (buf >> bit_idx) & 1;
            delete value;
            return ret;
        }

        BoolColumn* as_bool() {
            return dynamic_cast<BoolColumn*>(this);
        }
        /** Set value at idx. An out of bound idx is undefined.  */
        void set(size_t idx, bool val) {
            abort_if_not(idx < size(), "BoolColumn.set(): out of bounds");

            size_t chunk_idx = idx / CHUNK_SIZE;
            size_t item_idx = idx / (CHUNK_SIZE / sizeof(size_t));
            Key* chunk_key = chunk_keys_->get(chunk_idx);

            Value* value = kv_->get(*chunk_key);
            char* v = value->get();
            size_t buf;
            memcpy(&buf, v + item_idx * sizeof(size_t), sizeof(size_t));
            size_t bit_idx = idx % (sizeof(size_t) * 8); // number of bits in size_t

            if (val) {
                buf |= (1 << bit_idx);
            } else {
                buf &= (~(1 << bit_idx));
            }

            memcpy(v + item_idx * sizeof(size_t), &buf, sizeof(size_t));
            Value new_value(CHUNK_SIZE, v);
            kv_->put(*chunk_key, new_value);

            delete value;
        }

        char get_type_() {
            return BOOL;
        }
};

/*************************************************************************
 * IntColumn::
 * Holds int values.
 */
class IntColumn : public Column {
    public:
        IntColumn(String* col_name, KVStore* kv) : Column(col_name, kv) { }

        IntColumn(String* col_name, KVStore* kv, int n, ...) : Column(col_name, kv) {
            va_list arguments;
            va_start (arguments, n);

            for (int i = 0; i < n; i++ ) {
                push_back(va_arg( arguments, int));
            }
            va_end(arguments);
        }
        
        // NOTE: takes ownership of chunk_keys and the keys inside the vector
        IntColumn(size_t len, Vector<Key*>* chunk_keys, String* col_name, KVStore* kv) : Column(len, chunk_keys, col_name, kv) { }

        ~IntColumn() {
        }
        
        // gets the int at the index idx
        // if idx is out of bounds, exit
        int get(size_t idx) {
            abort_if_not(idx < size(), "IntColumn.get(): out of bounds");
            size_t chunk_idx = idx / CHUNK_SIZE;
            size_t item_idx = idx % CHUNK_SIZE;
            Key* chunk_key = chunk_keys_->get(chunk_idx);

            Value* val = kv_->get(*chunk_key);
            char* v = val->get();
            int rv = 0;
            memcpy(&rv, v + item_idx * sizeof(int), sizeof(int));
            delete val;
            return rv;
        }

        IntColumn* as_int() {
            return dynamic_cast<IntColumn*>(this);
        }

        void check_and_reallocate_() {
            // when the latest chunk is full
            if (len_ % CHUNK_SIZE == 0) {
                size_t chunk_idx = len_ / CHUNK_SIZE;

                char* new_chunk_key = generate_chunk_name(chunk_idx);
                Key* k = new Key(0, new_chunk_key);
                Value v(0);
                kv_->put(*k, v);

                chunk_keys_->push_back(k);
            }
        }

        virtual void push_back(int val) {
            check_and_reallocate_();
            size_t chunk_idx = len_ / CHUNK_SIZE;
            size_t item_idx = len_ % CHUNK_SIZE;
            Key* chunk_key = chunk_keys_->get(chunk_idx);

            Value* value = kv_->get(*chunk_key);
            char* v = value->get();
            memcpy(v + item_idx * sizeof(int), &val, sizeof(int));
            Value new_value(CHUNK_SIZE * sizeof(int), v);
            kv_->put(*chunk_key, new_value);

            len_++;
            delete value;
        }

        /** Set value at idx. An out of bound idx is undefined.  */
        void set(size_t idx, int val) {
            abort_if_not(idx < size(), "IntColumn.set(): Index out of bounds");
            size_t chunk_idx = idx / CHUNK_SIZE;
            size_t item_idx = idx % CHUNK_SIZE;
            Key* chunk_key = chunk_keys_->get(chunk_idx);

            Value* v = kv_->get(*chunk_key);
            char* val_buf = v->get();
            memcpy(val_buf + item_idx * sizeof(int), &val, sizeof(int));
            Value new_value(CHUNK_SIZE * sizeof(int), val_buf);
            kv_->put(*chunk_key, new_value);

            delete v;
        }

        char get_type_() {
            return INT;
        }
};

/*************************************************************************
 * DoubleColumn::
 * Holds double values.
 */
class DoubleColumn : public Column {
    public:
        DoubleColumn(String* col_name, KVStore* kv) : Column(col_name, kv) { }

        DoubleColumn(String* col_name, KVStore* kv, int n, ...) : Column(col_name, kv) {
            va_list arguments;
            va_start (arguments, n);

            for (int i = 0; i < n; i++ ) {
                double f = va_arg(arguments, double);
                push_back(f);
            }
            va_end(arguments);
        }
        
        // NOTE: takes ownership of chunk_keys and the keys inside the vector
        DoubleColumn(size_t len, Vector<Key*>* chunk_keys, String* col_name, KVStore* kv) : Column(len, chunk_keys, col_name, kv) { }

        ~DoubleColumn() { }

        void check_and_reallocate_() {
            // when the latest chunk is full
            if (len_ % CHUNK_SIZE == 0) {
                size_t chunk_idx = len_ / CHUNK_SIZE;

                char* new_chunk_key = generate_chunk_name(chunk_idx);

                Key* k = new Key(0, new_chunk_key);
                Value v(0);
                kv_->put(*k, v);

                chunk_keys_->push_back(k);
            }
        }
        
        // gets the double at the index idx
        // if idx is out of bounds, exit
        double get(size_t idx) {
            abort_if_not(idx < size(), "DoubleColumn.get(): index out of bounds");
            size_t chunk_idx = idx / CHUNK_SIZE;
            size_t item_idx = idx % CHUNK_SIZE;
            Key* chunk_key = chunk_keys_->get(chunk_idx);

            Value* val = kv_->get(*chunk_key);
            char* v = val->get();
            double rv = 0;
            memcpy(&rv, v + item_idx * sizeof(double), sizeof(double));
            delete val;
            return rv;
        }

        DoubleColumn* as_double() {
            return dynamic_cast<DoubleColumn*>(this);
        }

        virtual void push_back(double val) {
            check_and_reallocate_();
            size_t chunk_idx = len_ / CHUNK_SIZE;
            size_t item_idx = len_ % CHUNK_SIZE;
            Key* chunk_key = chunk_keys_->get(chunk_idx);

            Value* value = kv_->get(*chunk_key);
            char* v = value->get();
            memcpy(v + item_idx * sizeof(double), &val, sizeof(double));
            Value new_value(CHUNK_SIZE * sizeof(double), v);
            kv_->put(*chunk_key, new_value);

            len_++;
            delete value;
        }

        /** Set value at idx. An out of bound idx is undefined.  */
        void set(size_t idx, double val) {
            abort_if_not(idx < size(), "DoubleColumne.set(): Index out of bounds");
            size_t chunk_idx = idx / CHUNK_SIZE;
            size_t item_idx = idx % CHUNK_SIZE;
            Key* chunk_key = chunk_keys_->get(chunk_idx);

            Value* v = kv_->get(*chunk_key);
            char* val_buf = v->get();
            memcpy(val_buf + item_idx * sizeof(double), &val, sizeof(double));
            Value new_value(CHUNK_SIZE * sizeof(double), val_buf);
            kv_->put(*chunk_key, new_value);

            delete v;
        }

        char get_type_() {
            return DOUBLE;
        }
};

/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are owned and copied.  Nullptr is a valid
 * value.
 */
class StringColumn : public Column {
    public:
        StringColumn(String* col_name, KVStore* kv) : Column(col_name, kv) { }

        StringColumn(String* col_name, KVStore* kv, int n, ...) : Column(col_name,kv) {
            va_list arguments;
            va_start (arguments, n);

            for (int i = 0; i < n; i++ ) {
                String* tmp = va_arg(arguments, String*);
                push_back(tmp);
            }
            va_end(arguments);
        }
        
        // NOTE: takes ownership of chunk_keys and the keys inside the vector
        StringColumn(size_t len, Vector<Key*>* chunk_keys, String* col_name, KVStore* kv) : Column(len, chunk_keys, col_name, kv) { }

        ~StringColumn() {
            // delete all strings
            for (size_t i = 0; i < size(); i++) {
                delete get(i);  // todo?
            }
        }

        StringColumn* as_string() {
            return dynamic_cast<StringColumn*>(this);
        }

        void check_and_reallocate_() {
            // when the latest chunk is full
            if (len_ % CHUNK_SIZE == 0) {
                size_t chunk_idx = len_ / CHUNK_SIZE;

                char* new_chunk_key = generate_chunk_name(chunk_idx);

                Key* k = new Key(0, new_chunk_key);
                Value v(0);
                kv_->put(*k, v);

                chunk_keys_->push_back(k);
            }
        }
        
        // gets the String at the index idx
        // if idx is out of bounds, exit
        String* get(size_t idx) {
            abort_if_not(idx < size(), "StringColumn.get(): index out of bounds");
            size_t chunk_idx = idx / CHUNK_SIZE;
            size_t item_idx = idx % CHUNK_SIZE;
            Key* chunk_key = chunk_keys_->get(chunk_idx);
            Value* v = kv_->get(*chunk_key);
            char* val_buf = v->get();

            for (size_t i = 0; i < item_idx; i++) {
                val_buf += (strlen(val_buf) + 1);
            }

            String* ret = new String(val_buf);
            delete v;
            return ret;
        }
                
        virtual void push_back(String* val) {
            abort_if_not(val != nullptr, "StringColumn.push_back(): val is nullptr");
            check_and_reallocate_();

            size_t chunk_idx = len_ / CHUNK_SIZE;
            size_t item_idx = len_ % CHUNK_SIZE;
            Key* chunk_key = chunk_keys_->get(chunk_idx);

            Value* v = kv_->get(*chunk_key);
            Value new_value(v->size() + val->size() + 1);
            char* val_buf = new_value.get();

            // copy in the old values
            memcpy(val_buf, v->get(), v->size());

            // copy in the new val that is added
            memcpy(val_buf + v->size(), val->c_str(), val->size() + 1);

            kv_->put(*chunk_key, new_value);
            delete v;
            len_++;
        }

        /** Out of bound idx is undefined. */
        void set(size_t idx, String* val) {
            abort_if_not(idx < size(), "StringColumn.set(): index out of bounds");
            abort_if_not(val != nullptr, "StringColumn.set(): val is nullptr");

            size_t chunk_idx = idx / CHUNK_SIZE;
            size_t item_idx = idx % CHUNK_SIZE;
            Key* chunk_key = chunk_keys_->get(chunk_idx);

            Value* v = kv_->get(*chunk_key);
            char* val_buf = v->get();
            
            size_t byte_count = 0;
            for (size_t i = 0; i < item_idx; i++) {
                byte_count += (strlen(val_buf) + 1);
                val_buf += (strlen(val_buf) + 1);
            }

            // size of string being added + size of all strings - size of replaced string
            Value new_value(val->size() + v->size() - strlen(val_buf));
            char* new_buf = new_value.get();
            
            memcpy(new_buf, v->get(), byte_count);
            new_buf += byte_count;
            memcpy(new_buf, val->c_str(), val->size() + 1);
            new_buf += (val->size() + 1);
            memcpy(new_buf, val_buf + strlen(val_buf) + 1, v->size() - byte_count - strlen(val_buf) - 1);

            kv_->put(*chunk_key, new_value);

            delete v;
        }

        char get_type_() {
            return STRING;
        }
};


Column* Column::deserialize(const char* buf, KVStore* kvs) {
    char type;
    size_t len;
    String* name;

    const char* buf_pointer = buf;
    memcpy(&type, buf_pointer, 1);
    buf_pointer += 1;
    
    memcpy(&len, buf_pointer, sizeof(size_t));
    buf_pointer += sizeof(size_t);

    name = new String(buf_pointer);
    buf_pointer += name->size() + 1;

    Vector<Key*>* keys = new Vector<Key*>();

    for (size_t i = 0; i < (len / CHUNK_SIZE) + 1; i++) {
        keys->push_back(Key::deserialize(buf_pointer));
        buf_pointer += keys->get(i)->serial_buf_size();
    }

    switch (type) {
        case BOOL:
            return new BoolColumn(len, keys, name, kvs);
        case INT:
            return new IntColumn(len, keys, name, kvs);
        case DOUBLE:
            return new DoubleColumn(len, keys, name, kvs);
        case STRING:
            return new StringColumn(len, keys, name, kvs);
        default:
            Sys::fail("Column:deserialize, invalid type of column");
            return nullptr;
    }
}