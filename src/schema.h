//lang:CwC
#pragma once

#include <stdlib.h>

#include "object.h"
#include "string.h"

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
        size_t types_len_;

        size_t num_rows_;

        /** Copying constructor */
        Schema(Schema& from) {
            num_rows_ = 0;
            types_len_ = 0;
            types_cap_ = from.types_cap_;
            types_ = new char[types_cap_];

            for (size_t i = 0; i < from.types_len_; i++) {
                add_column(from.col_type(i));
            }
        }

        /** Create an empty schema **/
        Schema() {
            types_cap_ = ARRAY_STARTING_CAP;
            types_len_ = 0;
            num_rows_ = 0;
            types_ = new char[types_cap_];
        }

        /** Create a schema from a string of types. A string that contains
        * characters other than those identifying the four type results in
        * undefined behavior. The argument is external, a nullptr argument is
        * undefined. **/
        Schema(const char* types) {
            types_cap_ = ARRAY_STARTING_CAP;
            types_len_ = 0;
            num_rows_ = 0;
            types_ = new char[types_cap_];

            for (size_t i = 0; i < strlen(types); i++) {
                add_column(types[i]);
            }
        }

        ~Schema() {
            delete[] types_;
        }

        void check_and_reallocate_() {
            if (types_len_ >= types_cap_) {
                types_cap_ *= 2;
                char* temp = new char[types_cap_];
                for (size_t i = 0; i < types_len_; i++) {
                    temp[i] = types_[i];
                }
                delete types_;
                types_ = temp;
            }
        }

        void add_row() {
            num_rows_++;
        }

        /** Add a column of the given type and name (can be nullptr), name
        * is external. Names are expectd to be unique, duplicates result
        * in undefined behavior. */
        void add_column(char typ) {
            check_and_reallocate_();
            types_[types_len_++] = typ;
        }

        /** Return type of column at idx. An idx >= width is undefined. */
        char col_type(size_t idx) {
            abort_if_not(idx < width(), "Schema.col_type(): out of bounds");
            return types_[idx];
        }

        /** The number of columns */
        size_t width() {
            return types_len_;
        }

        size_t length() {
            return num_rows_;
        }

        /**
         * Is this schema equal to the other object. 
         * Checks column type, row names, and col names
         * Used for testing.
         */
        bool equals(Object* o) {
            Schema* other = dynamic_cast<Schema*>(o);

            if (other == nullptr || other->width() != width()) {
                return false;
            }
            
            // check types
            for (size_t i = 0; i < width(); i++) {
                if (col_type(i) != other->col_type(i)) {
                    return false;
                }
            }
            
            return true;
        }
};
