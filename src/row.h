//lang:CwC
#pragma once

#include <stdlib.h>

#include "object.h"
#include "string.h"
#include "schema.h"
#include "helper.h"
#include "column.h"

/*****************************************************************************
 * Fielder::
 * A field vistor invoked by Row.
 */
class Fielder : public Object {
    public:
        Fielder() {}
        virtual ~Fielder() {}
        /** Called before visiting a row, the argument is the row offset in the
            dataframe. */
        virtual void start(size_t r) {}
        
        /** Called for fields of the argument's type with the value of the field. */
        virtual void accept(bool b) {}
        virtual void accept(float f) {}
        virtual void accept(int i) {}
        virtual void accept(String* s) {}
        
        /** Called when all fields have been seen. */
        virtual void done() {}
};
 
/*************************************************************************
 * Row::
 *
 * This class represents a single row of data constructed according to a
 * dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete rows. Internally a dataframe hold data in columns.
 * Rows have pointer equality.
 */
class Row : public Object {
    public:
        Schema s_;
        size_t row_idx_;
        Column** data_;
        
        /** Build a row following a schema. */
        Row(Schema& scm) : s_(scm) {
            row_idx_ = -1;

            // adds columns based on the type inside of the schema
            data_ = new Column*[s_.width()];
            for (size_t i = 0; i < s_.width(); i++) {
                switch (s_.col_type(i)) {
                    case BOOL:
                        data_[i] = new BoolColumn();
                        break;                   
                    case INT:
                        data_[i] = new IntColumn();
                        break;
                    case FLOAT:
                        data_[i] = new FloatColumn();
                        break;
                    case STRING:
                        data_[i] = new StringColumn();
                        break;                
                    default:
                        abort_if_not(false, "Row(Schema): bad schema");
                }
            }
        }

        // destructor
        ~Row() {
            for (size_t i = 0; i < s_.width(); i++) {
                delete data_[i];
            }
            delete[] data_;
        }
        
        /** Setters: set the given column with the given value. Setting a column with
            * a value of the wrong type is undefined. 
            * should pushback to the underlying columns if they are empty
            * should set to replace the value in the column if they already have a value
            * */
        void set(size_t col, int val) {
            abort_if_not(col < width(), "Row.set(int) out of bounds");
            IntColumn* ic = data_[col]->as_int();
            if (ic->size() == 0) {
                ic->push_back(val);
            } else {
                ic->set(0, val);
            }
        }

        void set(size_t col, float val) {
            abort_if_not(col < width(), "Row.set(float) out of bounds");
            FloatColumn* fc = data_[col]->as_float();
            if (fc->size() == 0) {
                fc->push_back(val);
            } else {
                fc->set(0, val);
            }
        }

        void set(size_t col, bool val) {
            abort_if_not(col < width(), "Row.set(bool) out of bounds");
            BoolColumn* bc = data_[col]->as_bool();
            if (bc->size() == 0) {
                bc->push_back(val);
            } else {
                bc->set(0, val);
            }
            
        }

        void set(size_t col, String* val) {
            abort_if_not(col < width(), "Row.set(String*) out of bounds");
            StringColumn* sc = data_[col]->as_string();
            if (sc->size() == 0) {
                sc->push_back(val);
            } else {
                sc->set(0, val);
            }
        }
        
        /** Set/get the index of this row (ie. its position in the dataframe. This is
         *  only used for informational purposes, unused otherwise */
        void set_idx(size_t idx) {
            row_idx_=idx;
        }

        // returns the idx of this row in the DataFrame
        size_t get_idx() {
            return row_idx_;
        }
        
        /** Getters: get the value at the given column. If the column is not
            * of the requested type, the result is undefined. 
            * exits if trying to access a column out of bounds
            * */
        int get_int(size_t col) {
            abort_if_not(col < width(), "Row.get_int(): out of bounds");
            IntColumn *temp = data_[col]->as_int();
            return temp->get(0);
        }
        bool get_bool(size_t col) {
            abort_if_not(col < width(), "Row.get_bool(): out of bounds");
            BoolColumn *temp = data_[col]->as_bool();
            return temp->get(0);
        }
        float get_float(size_t col) {
            abort_if_not(col < width(), "Row.get_float(): out of bounds");
            FloatColumn *temp = data_[col]->as_float();
            return temp->get(0);
        }
        String* get_string(size_t col) {
            abort_if_not(col < width(), "Row.get_string(): out of bounds");
            StringColumn *temp = data_[col]->as_string();
            return temp->get(0);
        }
        
        /** Number of fields in the row. */
        size_t width() {
            return s_.width();
        }
        
        /** Type of the field at the given position. An idx >= width is  undefined. */
        char col_type(size_t idx) {
            return s_.col_type(idx);
        }
        
        /** Given a Fielder, visit every field of this row. The first argument is
            * index of the row in the dataframe.
            * Calling this method before the row's fields have been set is undefined. */
        void visit(size_t idx, Fielder& f) {
            f.start(idx);
            for (size_t i = 0; i < width(); i++) {
                switch (s_.col_type(i)) {
                    case BOOL:
                        f.accept(data_[i]->as_bool()->get(0));
                        break;                   
                    case INT:
                        f.accept(data_[i]->as_int()->get(0));
                        break;
                    case FLOAT:
                        f.accept(data_[i]->as_float()->get(0));
                        break;
                    case STRING:
                        f.accept(data_[i]->as_string()->get(0));
                        break;                
                    default:
                        abort_if_not(false, "Row.visit(): bad schema");
                }
            }

            f.done();
        }
        
};
 
/*******************************************************************************
 *  Rower::
 *  An interface for iterating through each row of a data frame. The intent
 *  is that this class should subclassed and the accept() method be given
 *  a meaningful implementation. Rowers can be cloned for parallel execution.
 */
class Rower : public Object {
    public:
        /** This method is called once per row. The row object is on loan and
             should not be retained as it is likely going to be reused in the next
            call. The return value is used in filters to indicate that a row
            should be kept. */
        virtual bool accept(Row& r) {
            return true;
        }
        
        /** Once traversal of the data frame is complete the rowers that were
             split off will be joined.  There will be one join per split. The
            original object will be the last to be called join on. The join method
            is reponsible for cleaning up memory. */
        virtual void join_delete(Rower* other)  {
            delete other;
        }
};