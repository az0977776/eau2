//lang:CwC
#pragma once

#include <stdlib.h>

#include "row.h"
#include "object.h"
#include "schema.h"
#include "column.h"
#include "thread.h"

/*****************************************************************************
Helper classes for DataFrame
*****************************************************************************/
/*
 * DataFrameAddFielder is a subclass of Fielder
 * Used to help add Rows to a DataFrame
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
        
        void accept(float f) {
            abort_if_not(idx_ < num_cols_, "DataFrameAddFielder.accept(float): Too many fields in row for DataFrame");
            cols_[idx_]->push_back(f);
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
        
        void accept(float f) {
            p("<");
            p(f);
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
 * DataFrameOriginal::
 *
 * A DataFrameOriginal is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A DataFrameOriginal has a schema that
 * describes it.
 */
class DataFrameOriginal : public Object {
    public:
        Schema schema_;
        Column** cols_;
        size_t cols_len_;
        size_t cols_cap_;
        size_t num_cols_owned_; // this is used to tell which columns we have to delete

        /** Create a data frame with the same columns as the given df but with no rows or rownames */
        DataFrameOriginal(DataFrameOriginal& df) : schema_() {
            for (size_t i = 0; i < df.ncols(); i++) {
                schema_.add_column(df.schema_.col_type(i));
            }
            
            cols_cap_ = schema_.width() < 4 ? 4: schema_.width();
            cols_len_ = schema_.width();
            num_cols_owned_ = schema_.width();
            create_columns_by_schema_();
        }

        /** Create a data frame from a schema and columns. All columns are created
        * empty. */
        DataFrameOriginal(Schema& schema) : schema_(schema) {
            cols_cap_ = schema_.width() < 4 ? 4: schema_.width();
            cols_len_ = schema_.width();
            num_cols_owned_ = schema_.width();
            create_columns_by_schema_();
        }

        ~DataFrameOriginal() {
            // do not own any of the columns in this data frame
            // just release the memory for the array holding the column pointers
            for (size_t i = 0; i < num_cols_owned_; i++) {
                delete cols_[i];
            }
            delete[] cols_; 
        }

        // creates columns in DataFrameOriginal based on the Schema's types
        void create_columns_by_schema_() {
            cols_ = new Column*[cols_cap_];
            for (size_t i = 0; i < cols_len_; i++) {
                switch (schema_.col_type(i)) {
                    case BOOL:
                        cols_[i] = new BoolColumn();
                        break;                   
                    case INT:
                        cols_[i] = new IntColumn();
                        break;
                    case FLOAT:
                        cols_[i] = new FloatColumn();
                        break;
                    case STRING:
                        cols_[i] = new StringColumn();
                        break;                
                    default:
                        abort_if_not(false, "DataFrameOriginal(): bad schema");
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

        /** Returns the DataFrameOriginal's schema. Modifying the schema after a DataFrameOriginal
        * has been created in undefined. */
        Schema& get_schema() {
            return schema_;
        }

        /** Adds a column this DataFrameOriginal, updates the schema, the new column
        * is external, and appears as the last column of the DataFrameOriginal, the
        * name is optional and external. A nullptr colum is undefined. */
        void add_column(Column* col) {
            abort_if_not(col != nullptr, "DataFrameOriginal.add_column(): col is nullptr");
            abort_if_not(cols_len_ == 0 || col->size() == nrows(), "DataFrameOriginal.add_column(): DataFrameOriginal is not rectangular");
            schema_.add_column(col->get_type());
            for (size_t i = nrows(); cols_len_ == 0 && i < col->size(); i++) {
                // only add rows if it is the first column to be added
                schema_.add_row(); // make sure schema has the same shape as DataFrameOriginal
            }
            check_and_reallocate_();
            cols_[cols_len_] = col;
            cols_len_++;
        }

        /** Return the value at the given column and row. Accessing rows or
         *  columns out of bounds, or request the wrong type is undefined.*/
        int get_int(size_t col, size_t row) {
            abort_if_not(col < cols_len_, "DataFrameOriginal.get_int(): column index out of bounds");
            return cols_[col]->as_int()->get(row);
        }

        bool get_bool(size_t col, size_t row) {
            abort_if_not(col < cols_len_, "DataFrameOriginal.get_bool(): column index out of bounds");
            return cols_[col]->as_bool()->get(row);
        }

        float get_float(size_t col, size_t row) {
            abort_if_not(col < cols_len_, "DataFrameOriginal.get_float(): column index out of bounds");
            return cols_[col]->as_float()->get(row);
        }

        String* get_string(size_t col, size_t row) {
            abort_if_not(col < cols_len_, "DataFrameOriginal.get_string(): column index out of bounds");
            return cols_[col]->as_string()->get(row);
        }

        /** Set the value at the given column and row to the given value.
        * If the column is not of the right type or the indices are out of
        * bound, the result is undefined. */
        void set(size_t col, size_t row, int val) {
            abort_if_not(col < cols_len_, "DataFrameOriginal.set(int): column index out of bounds");
            cols_[col]->as_int()->set(row, val);
        }

        void set(size_t col, size_t row, bool val) {
            abort_if_not(col < cols_len_, "DataFrameOriginal.set(bool): column index out of bounds");
            cols_[col]->as_bool()->set(row, val);
        }

        void set(size_t col, size_t row, float val) {
            abort_if_not(col < cols_len_, "DataFrameOriginal.set(float): column index out of bounds");
            cols_[col]->as_float()->set(row, val);
        }

        void set(size_t col, size_t row, String* val) {
            abort_if_not(col < cols_len_, "DataFrameOriginal.set(String*): column index out of bounds");
            cols_[col]->as_string()->set(row, val);
        }

        /** Set the fields of the given row object with values from the columns at
        * the given offset.  If the row is not form the same schema as the
        * DataFrameOriginal, results are undefined.
        */
        void fill_row(size_t idx, Row& row) {
            abort_if_not(idx < nrows(), "DataFrameOriginal.fill_row(): row index is out of bounds");
            row.set_idx(idx);
            for (size_t i = 0; i < cols_len_; i++) {
                switch (schema_.col_type(i)) {
                    case BOOL:
                        row.set(i, cols_[i]->as_bool()->get(idx));
                        break;                   
                    case INT:
                        row.set(i, cols_[i]->as_int()->get(idx));
                        break;
                    case FLOAT:
                        row.set(i, cols_[i]->as_float()->get(idx));
                        break;
                    case STRING:
                        row.set(i, cols_[i]->as_string()->get(idx));
                        break;                
                    default:
                        abort_if_not(false, "DataFrameOriginal.fill_row(): bad schema");
                }
            }
        }

        /** Add a row at the end of this dataframe. The row is expected to have
         *  the right schema and be filled with values, otherwise undedined.  */
        void add_row(Row& row) {
            DataFrameAddFielder f(cols_len_, cols_); 
            row.visit(schema_.length(), f); // add data to columns
            if (cols_len_ > 0 && cols_[0]->size() > schema_.length()) {
                schema_.add_row(); // nameless row
            }
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
        }

        /** Visit rows in order */
        void map(Rower& r) {
            map_rows_(0, nrows(), r);
        }

        /** Create a new dataframe, constructed from rows for which the given Rower
        * returned true from its accept method. 
        * The returned DataFrame will lose its row names.
        * */
        DataFrameOriginal* filter(Rower& r) {
            DataFrameOriginal* df = new DataFrameOriginal(*this);
            Row row(schema_);
            for (size_t i = 0; i < nrows(); i++) {
                fill_row(i, row);
                if (r.accept(row)) {
                    df->add_row(row);
                }
            }
            return df;
        }

        /** Print the dataframe in SoR format to standard output. */
        void print() {
            PrintDataFrameRower rower;
            map(rower);
        }

        bool equals(Object* o) {
            DataFrameOriginal* other = dynamic_cast<DataFrameOriginal*>(o);
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
};

// MapThread is a subclass of Thread
// MapThread is is used by pmap in the DataFrame class
// Each MapThread maps a Rower over a set of rows in the DatFrame
class MapThread : public Thread {
    public:
        size_t start_;
        size_t end_;
        Rower *r_;
        DataFrameOriginal *df_;

        MapThread(size_t start, size_t end, Rower *r, DataFrameOriginal *df) {
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

class DataFrame: public DataFrameOriginal{
    public:

        DataFrame(DataFrame& df) : DataFrameOriginal(df) {

        }

        /** Create a data frame from a schema and columns. All columns are created
        * empty. */
        DataFrame(Schema& schema) : DataFrameOriginal(schema) {

        }

        /** This method clones the Rower and executes the map in parallel. Join is
         * used at the end to merge the results. 
         * */
        void pmap(Rower& r) {
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
};
