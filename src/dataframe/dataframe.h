//lang:CwC
#pragma once

#include <stdlib.h>

#include "row.h"
#include "schema.h"
#include "column.h"

#include "../util/object.h"
#include "../util/string.h"
#include "../util/thread.h"

#include "../kvstore/keyvaluestore.h"
#include "../kvstore/keyvalue.h"

/*****************************************************************************
Helper classes for DataFrame
*****************************************************************************/
/*
 * DataFrameAddFielder is a subclass of Fielder
 * Used to add Rows to a DataFrame
 * @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
 */
class DataFrameAddFielder : public Fielder {
    public:
        Column** cols_;  // external
        size_t num_cols_;
        size_t idx_;

        DataFrameAddFielder(size_t num_cols, Column** cols) : Fielder() {
            cols_ = cols;
            num_cols_ = num_cols;
            idx_ = 0;
        }

        ~DataFrameAddFielder() { }
        
        /** Called before visiting a row, the argument is the row offset in the
            dataframe. */
        void start(size_t r) {
            // do nothing with this
            idx_ = 0;
        }
        
        /** Called for fields of the argument's type with the value of the field. */
        void accept(bool b) {
            abort_if_not(idx_ < num_cols_, "DataFrameAddFielder.accept(bool): Too many fields in row for DataFrame");
            cols_[idx_]->push_back(b);
            idx_++;
        }
        
        void accept(double d) {
            abort_if_not(idx_ < num_cols_, "DataFrameAddFielder.accept(double): Too many fields in row for DataFrame");
            cols_[idx_]->push_back(d);
            idx_++;
        }

        void accept(int i) {
            abort_if_not(idx_ < num_cols_, "DataFrameAddFielder.accept(int): Too many fields in row for DataFrame");
            cols_[idx_]->push_back(i);
            idx_++;
        }

        void accept(String* s) {
            abort_if_not(idx_ < num_cols_, "DataFrameAddFielder.accept(String*): Too many fields in row for DataFrame");
            cols_[idx_]->push_back(s);
            idx_++;
        }
        
        /** Called when all fields have been seen. */
        void done() {
            // handle if all columns are not filled in TODO
            abort_if_not(idx_ == num_cols_, "DataFrameAddFielder.done(): Did not fill in all columns");  // TODO: could do nothing
        }
};

/*
 * PrintDataFrameFielder is a subclass of Fielder
 * Used to help print fields in a DataFrame
 * @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
 */
class PrintDataFrameFielder : public Fielder {
    public:
        
        /** Called before visiting a row, the argument is the row offset in the
            dataframe. */
        void start(size_t r) {
            // do nothing with this
        }
        
        /** Called for fields of the argument's type with the value of the field. */
        void accept(bool b) {
            p("<");
            p(b);
            p(">");
        }
        
        void accept(double d) {
            p("<");
            p(d);
            p(">");
        }

        void accept(int i) {
            p("<");
            p(i);
            p(">");
        }

        void accept(String* s) {
            p("<\"");
            if (s != nullptr) {
                p(s->c_str());
            }
            p("\">");
        }
        
        /** Called when all fields have been seen. */
        void done() {
            // pass
        }
};

/**
 * PrintDataFrameRower is a subclass of Rower
 * Used to help print a row 
 * @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
 */
class PrintDataFrameRower : public Rower {
    public:
        PrintDataFrameFielder* f_;
        size_t row_idx;

        PrintDataFrameRower() { 
            f_ = new PrintDataFrameFielder();
            row_idx = 0;
        }

        ~PrintDataFrameRower() { 
            delete f_;
        }

        /** This method is called once per row. The row object is on loan and
             should not be retained as it is likely going to be reused in the next
            call. The return value is used in filters to indicate that a row
            should be kept. */
        virtual bool accept(Row& r) {
            // vist row using the PrintDataFrameFielder
            r.visit(row_idx++, *f_);
            pln("");
            return true;
        }
};

/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A DataFrame has a schema that
 * describes it.
 * @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
 */
class DataFrame : public Object {
    public:
        Schema schema_;
        Column** cols_;
        size_t cols_len_;
        size_t cols_cap_;
        size_t num_cols_owned_; // this is used to tell which columns we have to delete
        KVStore* kv_;  // external
        Key* key_;  // owned

        /** Create a data frame with the same columns as the given df but with no rows or rownames */
        DataFrame(DataFrame& df, Key& key) : schema_() {
            for (size_t i = 0; i < df.ncols(); i++) {
                schema_.add_column(df.schema_.col_type(i));
            }

            kv_ = df.kv_;
            key_ = key.clone();
            
            cols_cap_ = schema_.width() < 4 ? 4: schema_.width();
            cols_len_ = schema_.width();
            num_cols_owned_ = schema_.width();
            create_columns_by_schema_();

            add_self_to_kv_();
        }

        /** Create a data frame from a schema and columns. All columns are created
        * empty. */
        DataFrame(Schema& schema, Key& key, KVStore* kv) : DataFrame(schema, key, kv, true) {

        }

        DataFrame(Schema& schema, Key& key, KVStore* kv, bool add_self) : schema_(schema) {
            cols_cap_ = schema_.width() < 4 ? 4: schema_.width();
            cols_len_ = schema_.width();
            num_cols_owned_ = schema_.width();
            key_ = key.clone();
            kv_ = kv;
            
            create_columns_by_schema_();

            if (add_self) {
                add_self_to_kv_();
            }
        }

        ~DataFrame() {
            delete key_;
            // do not own any of the columns in this data frame
            // just release the memory for the array holding the column pointers
            for (size_t i = 0; i < num_cols_owned_; i++) {
                delete cols_[i];
            }
            delete[] cols_; 
        }

        void add_self_to_kv_() {
            char* serialized_df = serialize();
            Value v(serial_buf_size(), serialized_df);
            kv_->put(*key_, v);
            delete[] serialized_df; 
        }

        Key* get_key() {
            return key_->clone();
        }

        // full name to look like "DF_NAME:0x123ADF:0x321FDA"
        char* get_column_name_(size_t col_idx) {
            String* df_name = key_->get_name();
            size_t df_name_len = df_name->size();
            char* ret = new char[df_name_len + 3 + (2 * sizeof(size_t))];
            memcpy(ret, df_name->c_str(), df_name_len);
            ret[df_name_len] = ':';
            sprintf(ret+df_name_len+1, "0x%X", (unsigned int)col_idx);

            delete df_name;
            return ret;
        }

        // creates columns in DataFrame based on the Schema's types
        void create_columns_by_schema_() {
            MutableString str("");
            cols_ = new Column*[cols_cap_];
            for (size_t i = 0; i < cols_len_; i++) {
                str.set(get_column_name_(i));
                switch (schema_.col_type(i)) {
                    case BOOL:
                        cols_[i] = new BoolColumn(&str, kv_);
                        break;                   
                    case INT:
                        cols_[i] = new IntColumn(&str, kv_);
                        break;
                    case DOUBLE:
                        cols_[i] = new DoubleColumn(&str, kv_);
                        break;
                    case STRING:
                        cols_[i] = new StringColumn(&str, kv_);
                        break;                
                    default:
                        fail("DataFrame(): bad schema");
                }
            }
        }

        void check_and_reallocate_() {
            if (cols_len_ >= cols_cap_) {
                cols_cap_ *= 2;
                Column** temp = new Column*[cols_cap_];
                for (size_t i = 0; i < cols_len_; i++) {
                    temp[i] = cols_[i];
                }
                delete cols_;
                cols_ = temp;
            }
        }

        /** Returns the DataFrame's schema. Modifying the schema after a DataFrame
        * has been created in undefined. */
        Schema& get_schema() {
            return schema_;
        }

        /** Adds a column this DataFrame, updates the schema, the new column
        * is external, and appears as the last column of the DataFrame, the
        * name is optional and external. A nullptr colum is undefined. */
        void add_column(Column* col) {
            add_column(col, true);
        }

        /** Adds a column this DataFrame, updates the schema, the new column
        * is external, and appears as the last column of the DataFrame, the
        * name is optional and external. A nullptr colum is undefined. */
        void add_column(Column* col, bool add_self) {
            abort_if_not(col != nullptr, "DataFrame.add_column(): col is nullptr");
            abort_if_not(cols_len_ == 0 || col->size() == nrows(), "DataFrame.add_column(): DataFrame is not rectangular");
            schema_.add_column(col->get_type());
            for (size_t i = nrows(); cols_len_ == 0 && i < col->size(); i++) {
                // only add rows if it is the first column to be added
                schema_.add_row(); // make sure schema has the same shape as DataFrame
            }
            check_and_reallocate_();
            cols_[cols_len_] = col;
            cols_len_++;
            if (add_self) {
                add_self_to_kv_();
            }
        }

        /** Return the value at the given column and row. Accessing rows or
         *  columns out of bounds, or request the wrong type is undefined.*/
        int get_int(size_t col, size_t row) {
            abort_if_not(col < cols_len_, "DataFrame.get_int(): column index out of bounds");
            return cols_[col]->as_int()->get(row);
        }

        bool get_bool(size_t col, size_t row) {
            abort_if_not(col < cols_len_, "DataFrame.get_bool(): column index out of bounds");
            return cols_[col]->as_bool()->get(row);
        }

        double get_double(size_t col, size_t row) {
            abort_if_not(col < cols_len_, "DataFrame.get_double(): column index out of bounds");
            return cols_[col]->as_double()->get(row);
        }

        String* get_string(size_t col, size_t row) {
            abort_if_not(col < cols_len_, "DataFrame.get_string(): column index out of bounds");
            return cols_[col]->as_string()->get(row);
        }

        /** Set the fields of the given row object with values from the columns at
        * the given offset.  If the row is not form the same schema as the
        * DataFrame, results are undefined.
        */
        void fill_row(size_t idx, Row& row) {
            abort_if_not(idx < nrows(), "DataFrame.fill_row(): row index is out of bounds");
            row.set_idx(idx);
            for (size_t i = 0; i < cols_len_; i++) {
                switch (schema_.col_type(i)) {
                    case BOOL:
                        row.set(i, cols_[i]->as_bool()->get(idx));
                        break;                   
                    case INT:
                        row.set(i, cols_[i]->as_int()->get(idx));
                        break;
                    case DOUBLE:
                        row.set(i, cols_[i]->as_double()->get(idx));
                        break;
                    case STRING:
                        row.set(i, cols_[i]->as_string()->get(idx));
                        break;                
                    default:
                        fail("DataFrame.fill_row(): bad schema");
                }
            }
        }

        /** Add a row at the end of this dataframe. The row is expected to have
         *  the right schema and be filled with values, otherwise undedined.  */
        void add_row(Row& row, bool add_self) {
            DataFrameAddFielder f(cols_len_, cols_); 
            row.visit(schema_.length(), f); // add data to columns
            if (cols_len_ > 0 && cols_[0]->size() > schema_.length()) {
                schema_.add_row(); // nameless row
            }
            if (add_self) {
                add_self_to_kv_();
            }
        }

        /** Add a row at the end of this dataframe. The row is expected to have
         *  the right schema and be filled with values, otherwise undedined.  */
        void add_row(Row& row) {
            add_row(row, true);
        }


        /** The number of rows in the dataframe. */
        size_t nrows() {
            return schema_.length();
        }

        /** The number of columns in the dataframe.*/
        size_t ncols() {
            return cols_len_;
        }

        void map_rows_(size_t start, size_t end, Rower& r) {
            Row row(schema_);
            for (size_t i = start; i < end; i++) {
                fill_row(i, row);
                r.accept(row);
            }
            add_self_to_kv_();
        }

        /** Visit rows in order */
        void map(Rower& r) {
            map_rows_(0, nrows(), r);
        }

        /** Print the dataframe in SoR format to standard output. */
        void print() {
            PrintDataFrameRower rower;
            map(rower);
        }

        bool equals(Object* o) {
            DataFrame* other = dynamic_cast<DataFrame*>(o);
            if (other == nullptr || !schema_.equals(&other->schema_) || cols_len_ != other->cols_len_) {
                return false;
            }
            
            for (size_t i = 0; i < cols_len_; i++) {
                if (!cols_[i]->equals(other->cols_[i])) {
                    return false;
                }
            }
            return true;
        }

        size_t serial_buf_size() {
            size_t ret = key_->serial_buf_size() + sizeof(size_t); // key size and size_t for num columns
            for (size_t i = 0; i < ncols(); i++) {
                ret += cols_[i]->serial_buf_size();
            }
            return ret;
        }

        // <key><num_cols>[cols...]
        char* serialize(char* buf) {
            char* buf_pointer = buf;
            key_->serialize(buf_pointer);
            buf_pointer += key_->serial_buf_size();

            memcpy(buf_pointer, &cols_len_, sizeof(size_t));
            buf_pointer += sizeof(size_t);

            for (size_t i = 0; i < ncols(); i++) {
                cols_[i]->serialize(buf_pointer);
                buf_pointer += cols_[i]->serial_buf_size();
            }

            return buf;
        }

        // <key><num_cols>[cols...]
        char* serialize() {
            char* buf = new char[serial_buf_size()];
            return serialize(buf);
        }

        /** Create a new dataframe, constructed from rows for which the given Rower
        * returned true from its accept method. 
        * The returned DataFrame will lose its row names.
        * */
        DataFrame* filter(Rower& r) {
            Key k(0, "bogus name");
            return filter(r, k);
        }


        /** Create a new dataframe, constructed from rows for which the given Rower
        * returned true from its accept method. 
        * The returned DataFrame will lose its row names.
        * The given key is the name of the returned dataframe.
        * */
        DataFrame* filter(Rower& r, Key& key) {
            DataFrame* df = new DataFrame(*this, key);
            Row row(schema_);
            for (size_t i = 0; i < nrows(); i++) {
                fill_row(i, row);
                if (r.accept(row)) {
                    df->add_row(row);
                }
            }
            return df;
        }

        /** This method clones the Rower and executes the map in parallel. Join is
         * used at the end to merge the results. 
         * */
        void pmap(Rower& r);

        static DataFrame* fromArray(Key* k, KVStore* kvs, size_t size, String** vals) {
            Schema s("S");
            DataFrame* df = new DataFrame(s, *k, kvs, false);
            Row r(s);

            for (size_t i = 0; i < size; i++) {
                r.set(0, vals[i]);
                df->add_row(r, false);
            }
            
            df->add_self_to_kv_();
            
            return df;
        } 

        static DataFrame* fromArray(Key* k, KVStore* kvs, size_t size, double* vals) {
            Schema s("D");
            DataFrame* df = new DataFrame(s, *k, kvs, false);
            Row r(s);

            for (size_t i = 0; i < size; i++) {
                r.set(0, vals[i]);
                df->add_row(r, false);
            }

            df->add_self_to_kv_();

            return df;
        } 

        static DataFrame* fromArray(Key* k, KVStore* kvs, size_t size, int* vals) {
            Schema s("I");
            DataFrame* df = new DataFrame(s, *k, kvs, false);
            Row r(s);

            for (size_t i = 0; i < size; i++) {
                r.set(0, vals[i]);
                df->add_row(r, false);
            }

            df->add_self_to_kv_();

            return df;
        } 

        static DataFrame* fromArray(Key* k, KVStore* kvs, size_t size, bool* vals) {
            Schema s("B");
            DataFrame* df = new DataFrame(s, *k, kvs, false);
            Row r(s);

            for (size_t i = 0; i < size; i++) {
                r.set(0, vals[i]);
                df->add_row(r, false);
            }

            df->add_self_to_kv_();

            return df;
        } 

        static DataFrame* fromScalar(Key* k, KVStore* kvs, String* val) {
            return DataFrame::fromArray(k, kvs, 1, &val);
        } 

        static DataFrame* fromScalar(Key* k, KVStore* kvs, double val) {
            return DataFrame::fromArray(k, kvs, 1, &val);
        } 

        static DataFrame* fromScalar(Key* k, KVStore* kvs, int val) {
            return DataFrame::fromArray(k, kvs, 1, &val);
        } 

        static DataFrame* fromScalar(Key* k, KVStore* kvs, bool val) {
            return DataFrame::fromArray(k, kvs, 1, &val);
        } 

        static DataFrame* deserialize(const char* buf, KVStore* kvs) {
            Schema schema;
            size_t num_cols = 0;
            const char* buf_pointer = buf;
            
            Key* k = Key::deserialize(buf_pointer);
            buf_pointer += k->serial_buf_size();

            memcpy(&num_cols, buf_pointer, sizeof(size_t));
            buf_pointer += sizeof(size_t);

            // do not add the dataframe to the kvstore
            DataFrame* df = new DataFrame(schema, *k, kvs, false);
            Column* new_col;
            for (size_t i = 0; i < num_cols; i++) {
                new_col = Column::deserialize(buf_pointer, kvs);
                df->add_column(new_col, false);
                buf_pointer += df->cols_[i]->serial_buf_size();
            }

            df->num_cols_owned_ = df->ncols();

            delete k;
            return df;
        }
};

// MapThread is a subclass of Thread
// MapThread is is used by pmap in the DataFrame class
// Each MapThread maps a Rower over a set of rows in the DatFrame
class MapThread : public Thread {
    public:
        size_t start_;
        size_t end_;
        Rower *r_;
        DataFrame *df_;

        MapThread(size_t start, size_t end, Rower *r, DataFrame *df) {
            start_ = start;
            end_ = end;
            r_ = r;
            df_ = df;
        }

        /** Subclass responsibility, the body of the run method */
        virtual void run() { 
            df_->map_rows_(start_, end_, *r_);
        }
};

// this declaration must come after the declaration of MapThread
void DataFrame::pmap(Rower& r) {
    unsigned int n = get_thread_count(); // how many threads are availible on this host
    MapThread** pool = new MapThread*[n];
    Rower** rowers = new Rower*[n];

    size_t current_row = 0;
    size_t remander = nrows() % n;
    size_t dividend = nrows() / n;
    for (size_t i = 0; i < n; i++) {
        size_t num_rows = dividend;
        if ( i < remander ) {
            num_rows++;
        }

        rowers[i] = static_cast<Rower *>(r.clone());
        abort_if_not(rowers[i] != nullptr, "DataFrame.pmap(Rower): bad clone.");
        pool[i] = new MapThread(current_row, current_row + num_rows, rowers[i], this);
        pool[i]->start();

        current_row += num_rows;
    }

    for (size_t i = 0; i < n; i ++) {
        pool[i]->join();
        r.join_delete(rowers[i]);
        delete pool[i];
    }
    delete[] pool;
    delete[] rowers;
}