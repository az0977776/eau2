//lang:CwC
#pragma once

#include <stdlib.h>

#include "object.h"
#include "string.h"
#include "column.h"

// StringArray is a subclass of StringColumn
// holds String pointers that are external
class StringArray : public StringColumn {
    public:
        StringArray() : StringColumn() {}

        // returns the index of the first occurence of the str in the StringArray
        // returns -1 if not found
        int indexOf(const char* str) {
            String* s = new String(str);
            int ret_val = -1;
            for (size_t i = 0; i < size(); i++) {
                size_t big_idx = i / small_cap_;
                size_t small_idx = i % small_cap_;
                if (data_[big_idx][small_idx] && data_[big_idx][small_idx]->equals(s)) {
                    ret_val = i;
                    break;
                }
            }
            delete s;
            return ret_val;
        }

        // clone function that returns a copy of this StringArray
        StringArray* clone() {
            StringArray *temp = new StringArray();
            for (size_t i = 0; i < size(); i++) {
                temp->push_back(get(i));
            }
            return temp;
        }
};


/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 */
class Schema : public Object {
    public:
        char* types_;
        size_t types_cap_;

        // Uses StringArrays to hold the column and row names
        StringArray *row_names_;
        StringArray *col_names_;

        /** Copying constructor */
        Schema(Schema& from) {
            types_cap_ = from.types_cap_;
            row_names_ = from.row_names_->clone();
            col_names_ = from.col_names_->clone();
            
            types_ = new char[types_cap_];
            for (size_t i = 0; i < col_names_->size(); i++) {
                types_[i] = from.types_[i];
            }
        }

        /** Create an empty schema **/
        Schema() {
            types_cap_ = ARRAY_STARTING_CAP;
            types_ = new char[types_cap_];

            row_names_ = new StringArray();
            col_names_ = new StringArray();
        }

        /** Create a schema from a string of types. A string that contains
        * characters other than those identifying the four type results in
        * undefined behavior. The argument is external, a nullptr argument is
        * undefined. **/
        Schema(const char* types) {
            types_cap_ = ARRAY_STARTING_CAP;
            types_ = new char[types_cap_];

            row_names_ = new StringArray();
            col_names_ = new StringArray();

            for (size_t i = 0; i < strlen(types); i++) {
                add_column(types[i], nullptr);
            }
        }

        ~Schema() {
            delete[] types_;
            delete row_names_;
            delete col_names_;
        }

        void check_and_reallocate_() {
            if (width() >= types_cap_) {
                types_cap_ *= 2;
                char* temp = new char[types_cap_];
                for (size_t i = 0; i < width(); i++) {
                    temp[i] = types_[i];
                }
                delete types_;
                types_ = temp;
            }
        }

        /** Add a column of the given type and name (can be nullptr), name
        * is external. Names are expectd to be unique, duplicates result
        * in undefined behavior. */
        void add_column(char typ, String* name) {
            check_and_reallocate_();
            types_[width()] = typ;
            col_names_->push_back(name);
        }

        /** Add a row with a name (possibly nullptr), name is external.  Names are
         *  expectd to be unique, duplicates result in undefined behavior. */
        void add_row(String* name) {
            row_names_->push_back(name);
        }

        /** Return name of row at idx; nullptr indicates no name. An idx >= length
        * is undefined. */
        String* row_name(size_t idx) {
            return row_names_->get(idx);
        }

        /** Return name of column at idx; nullptr indicates no name given.
        *  An idx >= width is undefined.*/
        String* col_name(size_t idx) {
            return col_names_->get(idx);
        }

        /** Return type of column at idx. An idx >= width is undefined. */
        char col_type(size_t idx) {
            abort_if_not(idx < width(), "Schema.col_type(): out of bounds");
            return types_[idx];
        }

        /** Given a column name return its index, or -1. */
        int col_idx(const char* name) {
            if (name == nullptr) {
                return -1;
            }
            return col_names_->indexOf(name);
        }

        /** Given a row name return its index, or -1. */
        int row_idx(const char* name) {
            if (name == nullptr) {
                return -1;
            }
            return row_names_->indexOf(name);
        }

        /** The number of columns */
        size_t width() {
            return col_names_->size();
        }

        /** The number of rows */
        size_t length() {
            return row_names_->size();
        }

        /**
         * Is this schema equal to the other object. 
         * Checks column type, row names, and col names
         * Used for testing.
         */
        bool equals(Object* o) {
            Schema* other = dynamic_cast<Schema*>(o);

            if (other == nullptr || other->width() != width() || other->length() != length()) {
                return false;
            }
            
            // check types
            for (size_t i = 0; i < width(); i++) {
                if (col_type(i) != other->col_type(i)) {
                    return false;
                }
            }
            
            // check column names
            for (size_t i = 0; i < width(); i++) {
                if (col_name(i) == nullptr && other->col_name(i) == nullptr) {
                    continue;
                } else if (col_name(i) == nullptr || other->col_name(i) == nullptr) {
                    return false;
                } else if (!col_name(i)->equals(other->col_name(i))) {
                    return false;
                }
            }  
            
            // check row names
            for (size_t i = 0; i < length(); i++) {
                if (row_name(i) == nullptr && other->row_name(i) == nullptr) {
                    continue;
                } else if (row_name(i) == nullptr || other->row_name(i) == nullptr) {
                    return false;
                } else if (!row_name(i)->equals(other->row_name(i))) {
                    return false;
                }
            }   
            
            return true;
        }
};
