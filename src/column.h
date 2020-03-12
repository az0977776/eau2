//lang:CwC
#pragma once

#include <stdarg.h>
#include <stdlib.h>

#include "object.h"
#include "string.h"

static const size_t ARRAY_STARTING_CAP = 4;

enum ColumnType {
    UNKNOWN = 0,
    BOOL = 'B', 
    INT = 'I', 
    FLOAT = 'F',
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
        case FLOAT:
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

bool is_float(char *c) {
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

float as_float(char* c) {
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
    // check float
    if (is_float(c)) {
        return FLOAT;
    }
    return STRING;
}

class StringColumn;
class FloatColumn;
class IntColumn;
class BoolColumn;

/**************************************************************************
 * Column ::
 * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Columns are mutable, equality is pointer
 * equality. */
class Column : public Object {
    public:
        // ensures that small_cap_ is always divisible by sizeof(size_t) for our implementation of BoolColumn
        // capacity and length of an array inside the array of arrays
        const size_t small_cap_ = sizeof(size_t) * 16;
        size_t small_len_;

        // capaicty and length of array of arrays
        size_t big_cap_;
        size_t big_len_;

        Column() {
            big_cap_ = ARRAY_STARTING_CAP;
            big_len_ = 0;
            small_len_ = 0;
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
        
        virtual FloatColumn* as_float() {
            abort_if_not(false, "Column.as_float(): bad conversion");
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
        virtual void push_back(float val) {
            abort_if_not(false, "Column.push_back(float): push_back bad value");
        }
        virtual void push_back(String* val) {
            abort_if_not(false, "Column.push_back(String*): push_back bad value");
        }

        /** Returns the number of elements in the column. */
        virtual size_t size() {
            return big_len_ * small_cap_ + small_len_;
        }

        /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'. */
        char get_type() {
            return get_type_();
        }

        // helper function for get_type, is protected, should be overwritten in subclasses
        virtual char get_type_() {
            return 0;
        }
};
  

/*************************************************************************
 * BoolColumn::
 * Holds bool values.
 */
class BoolColumn : public Column {
    public:       
        size_t** data_;
     
        BoolColumn() : Column() {
            data_ = new size_t*[big_cap_];
            data_[0] = new size_t[small_cap_ / sizeof(size_t)];
        }

        BoolColumn(int n, ...) {
            data_ = new size_t*[big_cap_];
            data_[0] = new size_t[small_cap_ / sizeof(size_t)];
            
            va_list arguments;
            va_start (arguments, n);
            for (int i = 0; i <  n; i++ ) {
                bool b = va_arg(arguments, int);
                push_back(b);
            }
            va_end(arguments);
        }

        void check_and_reallocate_() {
            // if small array is full, move to next small array and start from 0
            if (small_len_ >= small_cap_) {
                small_len_ = 0;

                // if the big array is full, reallocate and copy over pointers to small arrays
                if (big_len_ + 1 >= big_cap_) {     
                    big_cap_ *= 2;
                    size_t** temp = new size_t*[big_cap_];
                    for (size_t i = 0; i <= big_len_; i++) {
                        temp[i] = data_[i];
                    } 
                    delete[] data_;
                    data_ = temp;
                } 

                big_len_++;
                // initialize the next small array
                data_[big_len_] = new size_t[small_cap_ / sizeof(size_t)];
            }   
        }

        virtual void push_back(bool val) {
            check_and_reallocate_();
            size_t item_idx = small_len_ / sizeof(size_t);
            size_t bit_idx = small_len_ % sizeof(size_t);
            if (val) {
                data_[big_len_][item_idx] |= (1 << bit_idx);
            } else {
                data_[big_len_][item_idx] &= (~(1 << bit_idx));
            }
            small_len_++;
        }
        
        // gets the bool at the index idx
        // if idx is out of bounds, exit
        bool get(size_t idx) {
            abort_if_not(idx < size(), "BoolColumn.get(): out of bounds");
            // find the array of arrays, the size_t in the array in the array of array, the bit in the size_t
            size_t big_idx = idx / small_cap_;
            size_t small_idx = (idx % small_cap_) / sizeof(size_t);
            size_t bit = idx % sizeof(size_t);
            return (data_[big_idx][small_idx] >> bit) & 1;
        }

        BoolColumn* as_bool() {
            return dynamic_cast<BoolColumn*>(this);
        }
        /** Set value at idx. An out of bound idx is undefined.  */
        void set(size_t idx, bool val) {
            abort_if_not(idx < size(), "BoolColumn.set(): out of bounds");
            size_t big_idx = idx / small_cap_;
            size_t small_idx = (idx % small_cap_) / sizeof(size_t);
            size_t bit = idx % sizeof(size_t);
            if (val) {
                data_[big_idx][small_idx] |= (1 << bit);
            } else {
                data_[big_idx][small_idx] &= (~(1 << bit));
            }
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
        int** data_;

        IntColumn() : Column() {
            data_ = new int*[big_cap_];
            data_[0] = new int[small_cap_];
        }

        IntColumn(int n, ...) : Column() {
            data_ = new int*[big_cap_];
            data_[0] = new int[small_cap_];
            
            va_list arguments;
            va_start (arguments, n);

            for (int i = 0; i < n; i++ ) {
                push_back(va_arg( arguments, int));
            }
            va_end(arguments);
        }

        ~IntColumn() {
            for (size_t i = 0; i < big_len_; i++) {
                delete[] data_[i];
            }
            delete[] data_;
        }
        
        // gets the int at the index idx
        // if idx is out of bounds, exit
        int get(size_t idx) {
            abort_if_not(idx < size(), "IntColumn.get(): out of bounds");
            size_t big_idx = idx / small_cap_;
            size_t small_idx = idx % small_cap_;
            return data_[big_idx][small_idx];
        }

        IntColumn* as_int() {
            return dynamic_cast<IntColumn*>(this);
        }

        void check_and_reallocate_() {
            // if small array is full, move to next small array and start from 0
            if (small_len_ >= small_cap_) {
                small_len_ = 0;

                // if the big array is full, reallocate and copy over pointers to small arrays
                if (big_len_ + 1 >= big_cap_) {     
                    big_cap_ *= 2;
                    int** temp = new int*[big_cap_];
                    for (size_t i = 0; i <= big_len_; i++) {
                        temp[i] = data_[i];
                    } 
                    delete[] data_;
                    data_ = temp;
                } 

                big_len_++;
                // initialize the next small array
                data_[big_len_] = new int[small_cap_];
            }   
        }

        virtual void push_back(int val) {
            check_and_reallocate_();
            data_[big_len_][small_len_] = val;
            small_len_++;
        }

        /** Set value at idx. An out of bound idx is undefined.  */
        void set(size_t idx, int val) {
            abort_if_not(idx < size(), "IntColumn.set(): Index out of bounds");
            size_t big_idx = idx / small_cap_;
            size_t small_idx = idx % small_cap_;
            data_[big_idx][small_idx] = val;
        }

        char get_type_() {
            return INT;
        }
};

/*************************************************************************
 * FloatColumn::
 * Holds float values.
 */
class FloatColumn : public Column {
    public:
        float** data_;

        FloatColumn() : Column() {
            data_ = new float*[big_cap_];
            data_[0] = new float[small_cap_];
        }

        FloatColumn(int n, ...) : Column() {
            data_ = new float*[big_cap_];
            data_[0] = new float[small_cap_];
            
            va_list arguments;
            va_start (arguments, n);

            for (int i = 0; i < n; i++ ) {
                float f = va_arg( arguments, double);
                push_back(f);
            }
            va_end(arguments);
        }

        ~FloatColumn() {
            for (size_t i = 0; i < big_len_; i++) {
                delete[] data_[i];
            }
            delete[] data_;
        }

        void check_and_reallocate_() {
            // if small array is full, move to next small array and start from 0
            if (small_len_ >= small_cap_) {
                small_len_ = 0;

                // if the big array is full, reallocate and copy over pointers to small arrays
                if (big_len_ + 1 >= big_cap_) {     
                    big_cap_ *= 2;
                    float** temp = new float*[big_cap_];
                    for (size_t i = 0; i <= big_len_; i++) {
                        temp[i] = data_[i];
                    } 
                    delete[] data_;
                    data_ = temp;
                } 

                big_len_++;
                // initialize the next small array
                data_[big_len_] = new float[small_cap_];
            }   
        }
        
        // gets the float at the index idx
        // if idx is out of bounds, exit
        float get(size_t idx) {
            abort_if_not(idx < size(), "FloatColumn.get(): index out of bounds");
            size_t big_idx = idx / small_cap_;
            size_t small_idx = idx % small_cap_;
            return data_[big_idx][small_idx];
        }

        FloatColumn* as_float() {
            return dynamic_cast<FloatColumn*>(this);
        }

        virtual void push_back(float val) {
            check_and_reallocate_();
            data_[big_len_][small_len_] = val;
            small_len_++;
        }

        /** Set value at idx. An out of bound idx is undefined.  */
        void set(size_t idx, float val) {
            size_t big_idx = idx / small_cap_;
            size_t small_idx = idx % small_cap_;
            abort_if_not(idx < size(), "FloatColumn.set(): index out of bounds");
            data_[big_idx][small_idx] = val;
        }

        char get_type_() {
            return FLOAT;
        }
};

/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are owned and copied.  Nullptr is a valid
 * value.
 */
class StringColumn : public Column {
    public:
        String*** data_;

        StringColumn() : Column() {
            data_ = new String**[big_cap_];
            data_[0] = new String*[small_cap_];     
        }

        StringColumn(int n, ...) : Column() {
            data_ = new String**[big_cap_];
            data_[0] = new String*[small_cap_]; 

            va_list arguments;
            va_start (arguments, n);

            for (int i = 0; i < n; i++ ) {
                push_back(va_arg(arguments, String*));
            }
            va_end(arguments);
        }

        ~StringColumn() {
            // delete all strings
            for (size_t i = 0; i < size(); i++) {
                delete get(i);
            }
            // delete chunks
            for (size_t i = 0; i < big_len_; i++) {
                delete[] data_[i];
            }
            // delete array of chunks
            delete[] data_;
        }

        StringColumn* as_string() {
            return dynamic_cast<StringColumn*>(this);
        }

        void check_and_reallocate_() {
            // if small array is full, move to next small array and start from 0
            if (small_len_ >= small_cap_) {
                small_len_ = 0;

                // if the big array is full, reallocate and copy over pointers to small arrays
                if (big_len_ + 1 >= big_cap_) {     
                    big_cap_ *= 2;
                    String*** temp = new String**[big_cap_];
                    for (size_t i = 0; i <= big_len_; i++) {
                        temp[i] = data_[i];
                    } 
                    delete[] data_;
                    data_ = temp;
                } 

                big_len_++;
                // initialize the next small array
                data_[big_len_] = new String*[small_cap_];
            }   
        }

        /** Returns the string at idx; undefined on invalid idx.*/
        String* get(size_t idx) {
            abort_if_not(idx < size(), "StringColumn.get(): index out of bounds");
            size_t big_idx = idx / small_cap_;
            size_t small_idx = idx % small_cap_;
            return data_[big_idx][small_idx];
        }
                
        virtual void push_back(String* val) {
            check_and_reallocate_();
            if (val == nullptr) {
                data_[big_len_][small_len_++] = nullptr;
            } else {
                data_[big_len_][small_len_++] = new String(*val);
            }
        }

        /** Out of bound idx is undefined. */
        void set(size_t idx, String* val) {
            size_t big_idx = idx / small_cap_;
            size_t small_idx = idx % small_cap_;
            abort_if_not(idx < size(), "StringColumn.get(): index out of bounds");
            if (data_[big_idx][small_idx] != nullptr) {
                delete data_[big_idx][small_idx];
            }
            if (val == nullptr) {
                data_[big_idx][small_idx] = nullptr;
            } else {
                data_[big_idx][small_idx] = new String(*val);
            }
        }

        char get_type_() {
            return STRING;
        }
};


