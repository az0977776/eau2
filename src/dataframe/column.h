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

#include "../util/config.h"

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
        KeyBuff* key_buff_;  // owned
        Array<Key>* chunk_keys_;

        size_t cached_chunk_idx_;
        Value* cached_chunk_value_;  // owned
        bool dirty_cache_; // this value is set to true when the cached chunk is mutated,
        // if this value is false when the cached chunk is changed, do not need to commit the chunk

        size_t len_;

        // this is only used to abstract common Column constructor
        Column(size_t len, KVStore* kv, String* col_name) {
            len_ = len;
            kv_ = kv;
            key_buff_ = new KeyBuff(col_name);

            dirty_cache_ = false;
            cached_chunk_idx_ = Config::MAX_SIZE_T;
            cached_chunk_value_ = nullptr; // owned by the column
            // the cached chunk is for both gets and puts, currently it does not update
            // if the remote chunk is updated (no cache invalidation) and will always 
            // overwrite whatever is in the kvstore when this chunk is commited
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

            delete key_buff_;
            for (size_t i = 0; i < chunk_keys_->size(); i++) {
                delete chunk_keys_->get(i);
            }

            delete chunk_keys_;
        }

        virtual size_t get_chunk_idx(size_t idx) {
            return idx / kv_->get_config().CHUNK_SIZE;
        }

        virtual size_t get_item_idx(size_t idx) {
            return idx % kv_->get_config().CHUNK_SIZE;
        }

        // checks if the chunk array needs to be expanded and expands it if true
        // the initial_chunk_size is the size of the new Value that is created during expansion
        virtual void check_and_reallocate_(size_t initial_chunk_size) {
            // when the latest chunk is full
            if (len_ % kv_->get_config().CHUNK_SIZE == 0) {
                size_t chunk_idx = get_chunk_idx(len_);

                Key* k = generate_chunk_key(chunk_idx);
                Value v(initial_chunk_size);
                kv_->put(*k, v);

                chunk_keys_->push_back(k);
            }
        }

        // puts the cached value into the kv store
        virtual void commit_cache() {
            if (!dirty_cache_) {
                return;
            }
            if (cached_chunk_value_ != nullptr){  
                put_(cached_chunk_idx_, *cached_chunk_value_);
                dirty_cache_ = false;
            }
        }

        template <class T>
        void push_back_(T val, bool commit) {
            check_and_reallocate_(kv_->get_config().CHUNK_SIZE * sizeof(T));
            size_t chunk_idx = get_chunk_idx(len_);
            size_t item_idx = get_item_idx(len_);

            // if getting a chunk with a different chunk_index than the cached chunk
            // index, function get_chunk_ will commit the old cached value
            Value* value = get_chunk_(chunk_idx);
            char* v = value->get();
            memcpy(v + item_idx * sizeof(T), &val, sizeof(T));
            dirty_cache_ = true;

            // if commit is true put the cached value into the KVStore
            if (commit){
                commit_cache();
            }

            len_++;
        }

        template<class T>
        T get_(size_t idx) {
            size_t chunk_idx = get_chunk_idx(idx);
            size_t item_idx = get_item_idx(idx);

            Value* val = get_chunk_(chunk_idx);

            char* v = val->get();
            T rv;
            memcpy(&rv, v + item_idx * sizeof(T), sizeof(T));

            return rv;
        }

        // puts the given value into the KVStore with the correct chunk key
        void put_(size_t chunk_idx, Value& value) {
            Key* chunk_key = chunk_keys_->get(chunk_idx);
            kv_->put(*chunk_key, value);
        }

        // colname:0x<hex_representation>
        Key* generate_chunk_key(size_t chunk_idx) {
            key_buff_->set_node_index(chunk_idx % kv_->num_nodes());
            return key_buff_->get(chunk_idx);
        }

        // chunk returned is owned by this column 
        Value* get_chunk_(size_t chunk_idx) {
            // if cache is empty or if the cached value's idx is not the same
            // get the value from the kvstore and cache it
            if (cached_chunk_value_ == nullptr || cached_chunk_idx_ != chunk_idx) {
                if (cached_chunk_value_ != nullptr) {
                    // delete the cached Value that is owned by this column
                    commit_cache();
                    delete cached_chunk_value_;
                }
                cached_chunk_idx_ = chunk_idx;
                Key* chunk_key = chunk_keys_->get(chunk_idx);
                cached_chunk_value_ = kv_->get(*chunk_key);  // returns the cloned value from KVStore
                dirty_cache_ = false;
            }
            return cached_chunk_value_;
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
        // virtual void push_back(int val) {
        //     push_back(val, true);
        // }
        // virtual void push_back(bool val) {
        //     push_back(val, true);
        // }
        // virtual void push_back(double val) {
        //     push_back(val, true);
        // }
        // virtual void push_back(String* val) {
        //     push_back(val, true);
        // }

        /** Type appropriate push_back methods. Calling the wrong method is
            * undefined behavior. **/
        virtual void push_back(int val, bool commit) {
            fail("Column.push_back(int, bool): push_back bad value");
        }
        virtual void push_back(bool val, bool commit) {
            fail("Column.push_back(bool, bool): push_back bad value");
        }
        virtual void push_back(double val, bool commit) {
            fail("Column.push_back(double, bool): push_back bad value");
        }
        virtual void push_back(String* val, bool commit) {
            fail("Column.push_back(String*, bool): push_back bad value");
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

        // get the next set of rows that are after the given end row index (inclusive) 
        // will set start_row_idx to the first row in the set, and end_row_idx to the row
        // after the the las row in the local set
        bool get_next_local_rows(size_t &start_row_idx, size_t &end_row_idx) {
            if (end_row_idx >= size()) {
                return false;
            }
            size_t chunk_idx = get_chunk_idx(end_row_idx);
            for (size_t i = chunk_idx; i < chunk_keys_->size(); i++) {
                if (chunk_keys_->get(i)->get_index() == kv_->node_index()) {
                    start_row_idx = i * kv_->get_config().CHUNK_SIZE;
                    end_row_idx = start_row_idx + kv_->get_config().CHUNK_SIZE;
                    end_row_idx = end_row_idx < size() ? end_row_idx : size();
                    return true;
                }
            }
            return false;
        }

        size_t serial_buf_size() {
            size_t ret = 1 + sizeof(size_t) + key_buff_->base_size() + 1; // char for type and size_t for length and the name of column
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

            memcpy(buf_pointer, key_buff_->get_base_c_str(), key_buff_->base_size() + 1);
            buf_pointer += key_buff_->base_size() + 1;
            
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
                push_back(b, false);
            }
            va_end(arguments);
            commit_cache();
        } 
        
        // NOTE: takes ownership of chunk_keys and the keys inside the Array
        BoolColumn(size_t len, Array<Key>* chunk_keys, String* col_name, KVStore* kv) : Column(len, chunk_keys, col_name, kv) { }

        ~BoolColumn() { }

        // which size_t to look for the bit in
        size_t get_item_idx(size_t idx) {
            return (idx % kv_->get_config().CHUNK_SIZE) / (sizeof(size_t) * 8);
        }

        virtual void push_back(bool val, bool commit) {
            check_and_reallocate_(kv_->get_config().CHUNK_SIZE / 8);
            size_t chunk_idx = get_chunk_idx(len_);
            size_t item_idx = get_item_idx(len_);
            size_t bit_idx = len_ % (sizeof(size_t) * 8);  // 8 bits per byte

            Value* value = get_chunk_(chunk_idx);
            char* v = value->get();

            size_t buf;
            size_t one = 1;
            memcpy(&buf, v + item_idx * sizeof(size_t), sizeof(size_t));

            if (val) {
                buf |= (one << bit_idx);
            } else {
                buf &= (~(one << bit_idx));
            }

            memcpy(v + item_idx * sizeof(size_t), &buf, sizeof(size_t));
            dirty_cache_ = true;

            if (commit) {
                commit_cache();
            }

            len_++;
        }

        // gets the bool at the index idx
        // if idx is out of bounds, exit
        bool get(size_t idx) {
            abort_if_not(idx < size(), "BoolColumn.get(): out of bounds");
            size_t chunk_idx = get_chunk_idx(idx);
            size_t item_idx = get_item_idx(idx);
            size_t bit_idx = idx % (sizeof(size_t) * 8); // number of bits in size_t
            
            Value* value = get_chunk_(chunk_idx);
            char* v = value->get();
            size_t buf;
            size_t one = 1;
            memcpy(&buf, v + item_idx * sizeof(size_t), sizeof(size_t));

            bool ret = (buf >> bit_idx) & one;

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
                push_back(va_arg( arguments, int), false);
            }
            va_end(arguments);
            commit_cache();
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

        virtual void push_back(int val, bool commit) {
            Column::push_back_<int>(val, commit);
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
                push_back(f, false);
            }
            va_end(arguments);
            commit_cache();
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

        virtual void push_back(double val, bool commit) {
            Column::push_back_<double>(val, commit);
        }

        // virtual void push_back(double val) {
        //     push_back(val, true);
        // }

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
        Array<String> string_cache_; // cache of all strings in the most recently accessed chunk

        StringColumn(String* col_name, KVStore* kv) : Column(col_name, kv), string_cache_() { }

        StringColumn(String* col_name, KVStore* kv, int n, ...) : Column(col_name,kv), string_cache_() {
            va_list arguments;
            va_start (arguments, n);

            for (int i = 0; i < n; i++ ) {
                String* tmp = va_arg(arguments, String*);
                push_back(tmp, false);
            }
            va_end(arguments);
            commit_cache();
        }
        
        // NOTE: takes ownership of chunk_keys and the keys inside the Array
        StringColumn(size_t len, Array<Key>* chunk_keys, String* col_name, KVStore* kv) : Column(len, chunk_keys, col_name, kv), string_cache_() { }

        ~StringColumn() { 
            clear_cache_();
        }

        void clear_cache_() {
            for (size_t i = 0; i < string_cache_.size(); i++) {
                delete string_cache_.get(i);
            }
            
            // clear
            string_cache_.clear();
        }

        void commit_cache() override {
            if (dirty_cache_) {
                size_t value_size = 0;
                for (size_t i = 0; i < string_cache_.size(); i++) {
                    value_size += (string_cache_.get(i)->size() + 1);
                }

                Value* value = new Value(value_size);
                char* buf = value->get();
                
                size_t buf_idx = 0;
                for (size_t i = 0; i < string_cache_.size(); i++) {
                    memcpy(buf + buf_idx, string_cache_.get(i)->c_str(), string_cache_.get(i)->size() + 1);
                    buf_idx += (string_cache_.get(i)->size() + 1);
                }

                // setup Column::cache  -- this is the only place in String column to touch Column::cache
                if (cached_chunk_value_ != nullptr) {
                    delete cached_chunk_value_;
                }
                // this should always be true
                // cached_chunk_idx_ = string_cached_chunk_idx_;
                cached_chunk_value_ = value;

                Column::commit_cache(); // call the parent commit
                dirty_cache_ = false;
            }
        }

        StringColumn* as_string() override {
            return dynamic_cast<StringColumn*>(this);
        }

        void update_string_cache(size_t chunk_idx) {
            if (chunk_idx != cached_chunk_idx_) {
                commit_cache();
                clear_cache_();

                // update Column::cache
                Value* v = get_chunk_(chunk_idx);
                char* val_buf = v->get();

                size_t byte_offset = 0;

                // update string_cache
                while(byte_offset < v->size()) {
                    // This is good performance for sequential reads 
                    string_cache_.push_back(new String(val_buf + byte_offset));
                    byte_offset += (strlen(val_buf + byte_offset) + 1);
                }
                dirty_cache_ = false;
            }
        }

        // gets the String at the index idx
        // if idx is out of bounds, exit
        String* get(size_t idx) {
            abort_if_not(idx < size(), "StringColumn.get(): index out of bounds");
            
            size_t chunk_idx = get_chunk_idx(idx);

            update_string_cache(chunk_idx);

            size_t item_idx = get_item_idx(idx);

            abort_if_not(item_idx < string_cache_.size(), "StringColumn.get(): Tried to get item %zu from chunk %zu with size %zu", item_idx, chunk_idx, string_cache_.size());
            return string_cache_.get(item_idx)->clone();
        }

        virtual void push_back(String* val, bool commit) override {
            abort_if_not(val != nullptr, "StringColumn.push_back(): val is nullptr");
            check_and_reallocate_(0);

            update_string_cache(get_chunk_idx(len_));

            string_cache_.push_back(val->clone());
            dirty_cache_ = true;

            if (commit) {
                commit_cache();
            }
            len_++;
        }

        char get_type_() override {
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

    if (len != 0) {
        for (size_t i = 0; i < (len / kvs->get_config().CHUNK_SIZE) + 1; i++) {
            keys->push_back(Key::deserialize(buf_pointer));
            buf_pointer += keys->get(i)->serial_buf_size();
        }
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