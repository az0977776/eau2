//lang:Cpp
#pragma once

#include <stdlib.h>

#include "schema.h"
#include "column.h"

#include "../util/object.h"
#include "../util/string.h"
#include "../util/helper.h"

/*****************************************************************************
 * Fielder::
 * A field vistor invoked by Row.
 * @note: this is still needed in order to add_row to a dataframe.
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
        virtual void accept(double d) {}
        virtual void accept(int i) {}
        virtual void accept(String* s) {}
        
        /** Called when all fields have been seen. */
        virtual void done() {}
};

class BoolBox;
class IntBox;
class DoubleBox;
class StringBox;

// for holding values of different types inside a Row class
// @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
class Box : public Object {
    public:
        bool has_been_set_;
        Box() { 
            has_been_set_ = false;
        }

        ~Box() {}

        virtual void set(bool val) {
            fail("Box.set(bool) Box not implemented");
        }

        virtual void set(int val) {
            fail("Box.set(int) Box not implemented");
        }

        virtual void set(double val) {
            fail("Box.set(double) Box not implemented");
        }

        virtual void set(String* val) {
            fail("Box.set(String*) Box not implemented");
        }
        
        virtual BoolBox* as_bool() {
            fail("Box is not a boolean");
            return nullptr;
        }

        virtual IntBox* as_int() {
            fail("Box is not a int");
            return nullptr;
        }

        virtual DoubleBox* as_double() {
            fail("Box is not a double");
            return nullptr;
        }

        virtual StringBox* as_string() {
            fail("Box is not a String");
            return nullptr;
        }
};

// A Box that holds bool values
// @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
class BoolBox: public Box {
    public:
        bool val_;

        BoolBox() : Box() {}

        ~BoolBox() {}

        BoolBox* as_bool() {
            return dynamic_cast<BoolBox*>(this);
        }
    
        void set(bool b) {
            has_been_set_ = true;
            val_ = b;
        }
        
        bool get() {
            abort_if_not(has_been_set_, "BoolBox.get(): has not been set yet");
            return val_;
        }
};

// A Box that holds an int value
// @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
class IntBox: public Box {
    public:
        int val_;

        IntBox() : Box() {}

        ~IntBox() {}

        IntBox* as_int() {
            return dynamic_cast<IntBox*>(this);
        }
    
        void set(int b) {
            has_been_set_ = true;
            val_ = b;
        }
        
        int get() {
            abort_if_not(has_been_set_, "IntBox.get(): has not been set yet");
            return val_;
        }
};

// a box that holds a double value
// @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
class DoubleBox: public Box {
    public:
        double val_;

        DoubleBox() : Box(){}

        ~DoubleBox() {}

        DoubleBox* as_double() {
            return dynamic_cast<DoubleBox*>(this);
        }
    
        void set(double b) {
            has_been_set_ = true;
            val_ = b;
        }
        
        double get() {
            abort_if_not(has_been_set_, "DoubleBox.get(): has not been set yet");
            return val_;
        }
};

// a Box that holds a string value
// @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
class StringBox: public Box {
    public:
        String* val_;

        StringBox() : Box() {}

        ~StringBox() {

        }

        StringBox* as_string() {
            return dynamic_cast<StringBox*>(this);
        }
    
        void set(String* b) {
            has_been_set_ = true;
            val_ = b;
        }
        
        String* get() {
            abort_if_not(has_been_set_, "StringBox.get(): has not been set yet");
            return val_;
        }
};

// Fielder that deletes strings inside of a row
class DeleteStringFielder : public Fielder {
    public:
        DeleteStringFielder() {}
        virtual ~DeleteStringFielder() {}
        /** Called before visiting a row, the argument is the row offset in the
            dataframe. */
        virtual void start(size_t r) { /* pass */ }
        
        /** Called for fields of the argument's type with the value of the field. */
        virtual void accept(bool b) { /* pass */ }
        virtual void accept(double d) { /* pass */ }
        virtual void accept(int i) { /* pass */ }
        virtual void accept(String* s) { delete s; }
        
        /** Called when all fields have been seen. */
        virtual void done() { /* pass */ }
};
 
/*************************************************************************
 * Row::
 *
 * This class represents a single row of data constructed according to a
 * dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete rows. Internally a dataframe hold data in columns.
 * Rows have pointer equality.
 * @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
 */
class Row : public Object {
    public:
        Schema s_;
        size_t row_idx_;
        Box** data_;
        
        /** Build a row following a schema. */
        Row(Schema& scm) : s_(scm) {
            row_idx_ = -1;

            // adds columns based on the type inside of the schema
            data_ = new Box*[s_.width()];
            for (size_t i = 0; i < s_.width(); i++) {
                switch (s_.col_type(i)) {
                    case BOOL:
                        data_[i] = new BoolBox();
                        break;                   
                    case INT:
                        data_[i] = new IntBox();
                        break;
                    case DOUBLE:
                        data_[i] = new DoubleBox();
                        break;
                    case STRING:
                        data_[i] = new StringBox();
                        break;                
                    default:
                        fail("Row(Schema): bad schema");
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

        // Delete all strings that are in this row. 
        void delete_strings() {
            DeleteStringFielder f;
            visit(get_idx(), f);
        }
        
        /** Setters: set the given column with the given value. Setting a column with
            * a value of the wrong type is undefined. 
            * should pushback to the underlying columns if they are empty
            * should set to replace the value in the column if they already have a value
            * */
        void set(size_t col, int val) {
            abort_if_not(col < width(), "Row.set(int) out of bounds");
            IntBox* ic = data_[col]->as_int();
            ic->set(val);
        }

        void set(size_t col, double val) {
            abort_if_not(col < width(), "Row.set(double) out of bounds");
            DoubleBox* fc = data_[col]->as_double();
            fc->set(val);
        }

        void set(size_t col, bool val) {
            abort_if_not(col < width(), "Row.set(bool) out of bounds");
            BoolBox* bc = data_[col]->as_bool();
            bc->set(val);
        }

        void set(size_t col, String* val) {
            abort_if_not(col < width(), "Row.set(String*) out of bounds");
            StringBox* sc = data_[col]->as_string();
            sc->set(val);
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
            IntBox *temp = data_[col]->as_int();
            return temp->get();
        }
        bool get_bool(size_t col) {
            abort_if_not(col < width(), "Row.get_bool(): out of bounds");
            BoolBox *temp = data_[col]->as_bool();
            return temp->get();
        }
        double get_double(size_t col) {
            abort_if_not(col < width(), "Row.get_double(): out of bounds");
            DoubleBox *temp = data_[col]->as_double();
            return temp->get();
        }
        String* get_string(size_t col) {
            abort_if_not(col < width(), "Row.get_string(): out of bounds");
            StringBox *temp = data_[col]->as_string();
            return temp->get();
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
                    {
                        bool b = data_[i]->as_bool()->get();
                        f.accept(b);
                        break;           
                    }        
                    case INT:
                    {
                        int in = data_[i]->as_int()->get();
                        f.accept(in);
                        break;
                    }
                    case DOUBLE:
                        f.accept(data_[i]->as_double()->get());
                        break;
                    case STRING:
                        f.accept(data_[i]->as_string()->get());
                        break;                
                    default:
                        fail("Row.visit(): bad schema");
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