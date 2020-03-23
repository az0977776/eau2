//lang:CwC
#pragma once
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#include "dataframe.h"
#include "schema.h"
#include "column.h"

#include "../util/object.h"
#include "../util/helper.h"

// The maximum length of a line buffer. No lines over 4095 bytes
static const int buff_len = 4096 * 16; 
static const int infer_line_count = 500;

// Reads a file and determines the schema on read
class SOR : public Object {
    public:
        FILE* file_;
        String* filename_;
        KVStore* kvs_;

        SOR(const char* filename, KVStore* kvs) { 
            kvs_ = kvs;
            filename_ = new String(filename);
            file_ = fopen(filename, "r");
            abort_if_not(file_ != NULL, "File is null pointer");
        }

        ~SOR() {
            fclose(file_);
            delete filename_;
        }
        
        // Reads in the data from the file starting at the from byte 
        // and reading at most len bytes
        DataFrame* read(size_t from, size_t len) {
            Key* key = new Key(0, filename_->c_str());
            Schema* schema = infer_columns_(from, len);
            DataFrame* df = new DataFrame(*schema, *key, kvs_);
            printf("before parsing the dataframe\n");
            parse_(df, from, len);
            printf("after parsing the df\n");
            delete schema;
            delete key;
            return df;
        }

        DataFrame* read() {
            // -1 is the max size_t value
            return read(0, -1);
        }

        // moves the file pointer to the start of the next line.
        void seek_(size_t from) {
            if (from == 0) {
                fseek(file_, from, SEEK_SET);
            } else {
                char buf[buff_len];
                fseek(file_, from - 1, SEEK_SET);
                fgets(buf, buff_len, file_);
            }
        }

        bool should_redefine_type_(char current_type, char inferred_type) {
            return column_type_to_num(current_type) < column_type_to_num(inferred_type);
        }

        // infers and creates the column objects
        Schema* infer_columns_(size_t from, size_t len) {
            seek_(from);
            char buf[buff_len];

            size_t total_bytes = 0;
            size_t row_count = 0;

            StrBuff col_types;

            while (fgets(buf, buff_len, file_) != nullptr && row_count < infer_line_count) {
                row_count++;
                total_bytes += strlen(buf);
                if (total_bytes >= len) {
                    break;
                }
                size_t num_fields;
                char** row = parse_row_(buf, &num_fields);

                for (size_t i = 0; i < num_fields; i++) {

                    char inferred_type = infer_type(row[i]);
                    if (should_redefine_type_(col_types.get(i), inferred_type)) {
                        col_types.set(i, inferred_type);
                    }
                }
                delete[] row;

            }

            String* schema_string = col_types.get();
            Schema* ret_val = new Schema(schema_string->c_str());
            delete schema_string;
            return ret_val;
        }

        // Find the start of the field value and null terminate it.
        // ASSUMPTION: input field is terminated by '>' char
        // NOTE: will mutate the field value
        // The value of len will be the offset of the null byte
        char* parse_field_(char* field, int* len) {
            char* ret = field;
            int j = 0;
            for (size_t i = 0; field[i] != '>'; i++) {
                switch (field[i]) {
                    case '<':  // Malformed input
                        affirm(false, "Multiple opening <");
                    case ' ': // extra space in front of field
                        ret++;
                        break;
                    case '"': // the start of a String
                        ret++;
                        j = i + 1;
                        while (field[j] != '"') { // add every character until a end quote is met.
                            j++;
                        }
                        field[j] = '\0';
                        *len = j;
                        return ret;
                    default:  // add every ASCII character to field return value.
                        for (j = i; field[j] != '>' && field[j] != ' '; j++);
                        field[j] = '\0';
                        *len = j;
                        return ret;  
                }
            }
            *len = 0;
            return nullptr;  // missing value
        }

        // parses a row and returns a list of field values as char*
        // NOTE: will mutate the row value.
        // The value of len will be the number of fields returned
        char** parse_row_(char* row, size_t *len) {
            int cap = 16;
            int l = 0;
            char** output = new char*[cap];

            for (size_t i = 0; row[i] != '\0'; i++) {
                if (row[i] == '<') {
                    int to_increment = 0;
                    if (l >= cap) {
                        cap*=2;
                        char** temp = new char*[cap];
                        for (int k = 0; k < l; k++) {
                            temp[k] = output[k];
                        }
                        delete[] output;
                        output = temp;
                    }

                    output[l++] = parse_field_(&row[i + 1], &to_increment);
                    i = i + to_increment + 1;
                }
            }

            *len = l;
            return output;
        }


        // read the rows from the starting byte up to len bytes into Columns.
        void parse_(DataFrame* df, size_t from, size_t len) {
            seek_(from);
            String empty_string("");
            char buf[buff_len];
            Schema schema = df->get_schema();
            Row df_row(schema);

            size_t total_bytes = 0;
            while (fgets(buf, buff_len, file_) != nullptr) {
                total_bytes += strlen(buf);
                if (total_bytes >= len) {
                    break;
                }

                size_t num_fields; 
                // current row could have more columns than infered - parse the frist len_ columns
                char** row = parse_row_(buf, &num_fields);
                // skipping rows with too few fields
                if (num_fields == 0) {
                    delete[] row;
                    continue;
                }

                // we skip the row as soon as we find a field that does not match our schema
                bool skip = false;
                for (size_t i = 0; i < df->ncols(); i++) {
                    if (i < num_fields && row[i] != nullptr && should_redefine_type_(schema.col_type(i), infer_type(row[i]))) {
                        skip = true;
                        break;
                    }
                }
                if (skip) {
                    delete[] row;
                    continue;
                }

                // add all fields in this row to columns
                for (size_t i = 0; i < df->ncols(); i++) {
                    if (i >= num_fields || row[i] == nullptr) {
                        switch(schema.col_type(i)) {
                            case BOOL:
                                df_row.set(i, false);
                                break;
                            case INT:
                                df_row.set(i, 0);
                                break;
                            case DOUBLE:
                                df_row.set(i, 0.0);
                                break;
                            case STRING:
                                df_row.set(i, &empty_string);
                                break;
                            default:
                                abort_if_not(false, "SOR.parse(): empty value into unknown col type");    
                        }
                    } else {
                        switch(schema.col_type(i)) {
                            case BOOL:
                            {
                                df_row.set(i, as_bool(row[i]));
                                break;
                            }
                            case INT:
                            {
                                df_row.set(i, as_int(row[i]));
                                break;
                            }
                            case DOUBLE:
                            {
                                df_row.set(i, as_double(row[i]));
                                break;
                            }
                            case STRING:
                            {
                                String* tmp = as_string(row[i]);
                                df_row.set(i, tmp);
                                delete tmp;
                                break;
                            }
                            default:
                            {
                                abort_if_not(false, "SOR.parse(): put value into unknown col type"); 
                            }   
                        }
                    }
                }
                df->add_row(df_row);
                delete[] row;
            }
        }
};