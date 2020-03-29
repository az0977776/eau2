//lang:Cpp
#pragma once

#include <stdarg.h>
#include <stdlib.h>

#include "../util/string.h"
#include "../util/object.h"
#include "../util/string.h"
#include "../util/array.h"

#include "../kvstore/keyvalue.h"
#include "../kvstore/keyvaluestore.h"

#include "../util/constant.h"

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

/**************************************************************************
 * Column ::
 * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Columns are mutable, equality is pointer
 * equality. 
 * @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
 * */
class Column : public Object {
    public:
        KVStore* kv_;  // external
        String* col_name_;  // owned
        Array<Key>* chunk_keys_;

        size_t cached_chunk_idx_;
        Value* cached_chunk_value_;  // owned

        size_t len_;

        // this is only used to abstract common Column constructor
        Column(size_t len, KVStore* kv, String* col_name) {
            len_ = len;
            kv_ = kv;
            col_name_ = col_name->clone();

            cached_chunk_idx_ = 0;
            cached_chunk_value_ = nullptr; // owned by the column
        }

        Column(String* col_name, KVStore* kv) : Column(0, kv, col_name) {
            chunk_keys_ = new Array<Key>();
        }

        // NOTE: takes ownership of chunk_keys and the keys inside the Array
        Column(size_t len, Array<Key>* chunk_keys, String* col_name, KVStore* kv) : Column(len, kv, col_name) {
            chunk_keys_ = chunk_keys;
        }

        ~Column() {
            if (cached_chunk_value_ != nullptr) {
                delete cached_chunk_value_;
            }

            delete col_name_;
            for (size_t i = 0; i < chunk_keys_->size(); i++) {
                delete chunk_keys_->get(i);
            }

            delete chunk_keys_;
        }

        virtual size_t get_chunk_idx(size_t idx) {
            return idx / CHUNK_SIZE;
        }

        virtual size_t get_item_idx(size_t idx) {
            return idx % CHUNK_SIZE;
        }

        // checks if the chunk array needs to be expanded and expands it if true
        // the initial_chunk_size is the size of the new Value that is created during expansion
        void check_and_reallocate_(size_t initial_chunk_size) {
            // when the latest chunk is full
            if (len_ % CHUNK_SIZE == 0) {
                size_t chunk_idx = get_chunk_idx(len_);

                char* new_chunk_key = generate_chunk_name(chunk_idx);

                Key* k = new Key(0, new_chunk_key);
                Value v(initial_chunk_size);
                kv_->put(*k, v);

                chunk_keys_->push_back(k);

                delete[] new_chunk_key;
            }
        }

        template <class T>
        void push_back_(T val) {
            check_and_reallocate_(CHUNK_SIZE * sizeof(T));
            bool owned = false;
            size_t chunk_idx = get_chunk_idx(len_);
            size_t item_idx = get_item_idx(len_);

            Value* value = get_chunk_(chunk_idx, owned);
            char* v = value->get();
            memcpy(v + item_idx * sizeof(T), &val, sizeof(T));

            Value new_value(CHUNK_SIZE * sizeof(T), v);

            put_(chunk_idx, new_value);

            len_++;
            if (owned) {
                delete value;
            }
        }

        template<class T>
        T get_(size_t idx) {
            bool owned = false;
            size_t chunk_idx = get_chunk_idx(idx);
            size_t item_idx = get_item_idx(idx);

            Value* val = get_chunk_(chunk_idx, owned);

            char* v = val->get();
            T rv;
            memcpy(&rv, v + item_idx * sizeof(T), sizeof(T));

            if (owned) {
                delete val;
            }
            return rv;
        }

        // puts the given value into the KVStore with the correct chunk key
        void put_(size_t chunk_idx, Value& value) {
            Key* chunk_key = chunk_keys_->get(chunk_idx);
            kv_->put(*chunk_key, value);
        }

        // for debugging
        void print_chunk_keys() {
            printf("Chunk keys for Column: %s\n", col_name_->c_str());
            for (size_t i = 0; i < chunk_keys_->size(); i++) {
                printf("   %s\n", chunk_keys_->get(i)->key_->c_str());
            }
        }

        // colname:0x<hex_representation>
        // each byte needs two characters to represent
        char* generate_chunk_name(size_t chunk_idx) {
            size_t col_name_len = col_name_->size();
            char* ret = new char[col_name_len + 3 + (sizeof(size_t) * 2)]; 
            memcpy(ret, col_name_->c_str(), col_name_len);
            ret[col_name_len] = ':';
            sprintf(ret+col_name_len+1, "0x%X", (unsigned int)chunk_idx);
            return ret;
        }

        // chunk returned is owned by this column 
        Value* get_chunk_(size_t chunk_idx, bool &owned) {
            // if getting the latest chunk (not read-only yet)
            if ((chunk_idx + 1) * CHUNK_SIZE > len_) {
                // always re-get the last chunk of the column because it could have changed and don't cache
                Key* chunk_key = chunk_keys_->get(chunk_idx);
                return kv_->get(*chunk_key, owned);
            // if getting a read-only chunk
            } else {
                // if cache is empty or if the cached value's idx is not the same
                // get the value from the kvstore and cache it
                if (cached_chunk_value_ == nullptr || cached_chunk_idx_ != chunk_idx) {
                    if (cached_chunk_value_ != nullptr) {
                        // delete the cached Value that is owned by this column
                        delete cached_chunk_value_;
                    }
                    cached_chunk_idx_ = chunk_idx;
                    Key* chunk_key = chunk_keys_->get(chunk_idx);
                    cached_chunk_value_ = kv_->get(*chunk_key);  // returns the cloned value from KVStore
                }
                owned = false; // owned by the column
                return cached_chunk_value_;
            }
        }

        /** Type converters: Return same column under its actual type, or
         *  nullptr if of the wrong type.  */
        virtual IntColumn* as_int() {
            fail("Column.as_int(): bad conversion");
            return nullptr;
        }

        virtual BoolColumn*  as_bool() {
            fail("Column.as_bool(): bad conversion");
            return nullptr;
        }
        
        virtual DoubleColumn* as_double() {
            fail("Column.as_double(): bad conversion");
            return nullptr;
        }

        virtual StringColumn* as_string() {
            fail("Column.as_string(): bad conversion");
            return nullptr;
        }

        /** Type appropriate push_back methods. Calling the wrong method is
            * undefined behavior. **/
        virtual void push_back(int val) {
            fail("Column.push_back(int): push_back bad value");
        }
        virtual void push_back(bool val) {
            fail("Column.push_back(bool): push_back bad value");
        }
        virtual void push_back(double val) {
            fail("Column.push_back(double): push_back bad value");
        }
        virtual void push_back(String* val) {
            fail("Column.push_back(String*): push_back bad value");
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
 * @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
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
        
        // NOTE: takes ownership of chunk_keys and the keys inside the Array
        BoolColumn(size_t len, Array<Key>* chunk_keys, String* col_name, KVStore* kv) : Column(len, chunk_keys, col_name, kv) { }

        ~BoolColumn() { }

        // which size_t to look for the bit in
        size_t get_item_idx(size_t idx) {
            return (idx % CHUNK_SIZE) / (sizeof(size_t) * 8);
        }

        virtual void push_back(bool val) {
            check_and_reallocate_(CHUNK_SIZE / 8);
            bool owned = false;
            size_t chunk_idx = get_chunk_idx(len_);
            size_t item_idx = get_item_idx(len_);
            size_t bit_idx = len_ % (sizeof(size_t) * 8);  // 8 bits per byte

            Value* value = get_chunk_(chunk_idx, owned);
            char* v = value->get();

            size_t buf;
            memcpy(&buf, v + item_idx * sizeof(size_t), sizeof(size_t));

            if (val) {
                buf |= (1 << bit_idx);
            } else {
                buf &= (~(1 << bit_idx));
            }

            memcpy(v + item_idx * sizeof(size_t), &buf, sizeof(size_t));
            Value new_value(CHUNK_SIZE, v);

            put_(chunk_idx, new_value);

            len_++;
            if (owned){
                delete value;
            }
        }
        
        // gets the bool at the index idx
        // if idx is out of bounds, exit
        bool get(size_t idx) {
            abort_if_not(idx < size(), "BoolColumn.get(): out of bounds");
            bool owned = false;
            size_t chunk_idx = get_chunk_idx(idx);
            size_t item_idx = get_item_idx(idx);
            size_t bit_idx = idx % (sizeof(size_t) * 8); // number of bits in size_t
            
            Value* value = get_chunk_(chunk_idx, owned);
            char* v = value->get();
            size_t buf;
            memcpy(&buf, v + item_idx * sizeof(size_t), sizeof(size_t));

            bool ret = (buf >> bit_idx) & 1;
            if (owned) {
                delete value;
            }
            return ret;
        }

        BoolColumn* as_bool() {
            return dynamic_cast<BoolColumn*>(this);
        }

        char get_type_() {
            return BOOL;
        }
};

/*************************************************************************
 * IntColumn::
 * Holds int values.
 * @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
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
        
        // NOTE: takes ownership of chunk_keys and the keys inside the Array
        IntColumn(size_t len, Array<Key>* chunk_keys, String* col_name, KVStore* kv) : Column(len, chunk_keys, col_name, kv) { }

        ~IntColumn() {
        }
        
        // gets the int at the index idx
        // if idx is out of bounds, exit
        int get(size_t idx) {
            abort_if_not(idx < size(), "IntColumn.get(): out of bounds");
            return Column::get_<int>(idx);
        }

        IntColumn* as_int() {
            return dynamic_cast<IntColumn*>(this);
        }

        virtual void push_back(int val) {
            Column::push_back_<int>(val);
        }

        char get_type_() {
            return INT;
        }
};

/*************************************************************************
 * DoubleColumn::
 * Holds double values.
 * @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
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
        
        // NOTE: takes ownership of chunk_keys and the keys inside the Array
        DoubleColumn(size_t len, Array<Key>* chunk_keys, String* col_name, KVStore* kv) : Column(len, chunk_keys, col_name, kv) { }

        ~DoubleColumn() { }
        
        // gets the double at the index idx
        // if idx is out of bounds, exit
        double get(size_t idx) {
            abort_if_not(idx < size(), "DoubleColumn.get(): index of %zu out of bounds (Column size = %zu)", idx, size());
            return Column::get_<double>(idx);
        }

        DoubleColumn* as_double() {
            return dynamic_cast<DoubleColumn*>(this);
        }

        virtual void push_back(double val) {
            Column::push_back_<double>(val);
        }

        char get_type_() {
            return DOUBLE;
        }
};

/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are owned and copied.  Nullptr is a valid
 * value.
 * @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
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
        
        // NOTE: takes ownership of chunk_keys and the keys inside the Array
        StringColumn(size_t len, Array<Key>* chunk_keys, String* col_name, KVStore* kv) : Column(len, chunk_keys, col_name, kv) { }

        ~StringColumn() {
            // delete all strings
            for (size_t i = 0; i < size(); i++) {
                delete get(i);  // todo?
            }
        }

        StringColumn* as_string() {
            return dynamic_cast<StringColumn*>(this);
        }
        
        // gets the String at the index idx
        // if idx is out of bounds, exit
        String* get(size_t idx) {
            abort_if_not(idx < size(), "StringColumn.get(): index out of bounds");
            bool owned = false;
            size_t chunk_idx = get_chunk_idx(idx);
            size_t item_idx = get_item_idx(idx);
            
            Value* v = get_chunk_(chunk_idx, owned);
            char* val_buf = v->get();

            for (size_t i = 0; i < item_idx; i++) {
                val_buf += (strlen(val_buf) + 1);
            }

            String* ret = new String(val_buf);
            if (owned) {
                delete v;
            }
            return ret;
        }
                
        virtual void push_back(String* val) {
            abort_if_not(val != nullptr, "StringColumn.push_back(): val is nullptr");
            check_and_reallocate_(0);
            bool owned = false;

            size_t chunk_idx = get_chunk_idx(len_);

            Value* v = get_chunk_(chunk_idx, owned);
            Value new_value(v->size() + val->size() + 1);
            char* val_buf = new_value.get();

            // copy in the old values
            memcpy(val_buf, v->get(), v->size());

            // copy in the new val that is added
            memcpy(val_buf + v->size(), val->c_str(), val->size() + 1);

            put_(chunk_idx, new_value);

            if (owned) {
                delete v;
            }
            len_++;
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

    Array<Key>* keys = new Array<Key>();

    for (size_t i = 0; i < (len / CHUNK_SIZE) + 1; i++) {
        keys->push_back(Key::deserialize(buf_pointer));
        buf_pointer += keys->get(i)->serial_buf_size();
    }

    Column* ret = nullptr;

    switch (type) {
        case BOOL:
            ret = new BoolColumn(len, keys, name, kvs);
            break;
        case INT:
            ret = new IntColumn(len, keys, name, kvs);
            break;
        case DOUBLE:
            ret = new DoubleColumn(len, keys, name, kvs);
            break;
        case STRING:
            ret = new StringColumn(len, keys, name, kvs);
            break;
        default:
            Sys::fail("Column:deserialize, invalid type of column");
            break;
    }

    delete name;  // was cloned when creating the column
    return ret;
}