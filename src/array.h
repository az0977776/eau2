//lang::CwC
#pragma once

#include "object.h"
#include <stdlib.h>

// @author barth.c@husky.neu.edu
template <class T>
class Array : public Object{
    public:
        T** values_; // own array but not Objects in array
        size_t len_, cap_;

        // initializes an empty array with a size of 0 and a max size of 2
        Array() {
            cap_ = 2;
            len_ = 0;
            values_ = new T*[cap_];
        }

        Array(size_t length){
            if (length == 0) {
                cap_ = 2;
            } else {
                cap_ = length;
            }
            len_ = 0;
            values_ = new T*[cap_];
        }

        // deletes the array
        virtual ~Array() {
            // the user of Array class must get all elements and delete them
            delete[] values_;
        }

        // checks if the underlying array needs to be reallocated
        // does so if true
        virtual void check_reallocate_() {
            if (len_ >= cap_) {
                cap_*=2;
                T **temp = new T*[cap_];
                for (size_t i = 0; i < len_; i++) {
                    temp[i] = values_[i];
                }
                delete[] values_;
                values_ = temp;
            }
        }

        // returns all values in array
        virtual T** get_all() {
            T **ret = new T*[len_];
            for (size_t i = 0; i < len_; i++) {
                ret[i] = values_[i];
            }
            return ret;
        }

        // return the object at the index, returns nullptr if the index > size
        virtual T* get(size_t index){
            if (index > len_) {
                return nullptr;
            } else {
                return values_[index];
            }
        }

        virtual void push(T* obj) {
            check_reallocate_();
            values_[len_] = obj;  // not copying on add ...
            len_++;
        }

        //  sets the element at index i to the new object, pushes object onto end of array if i > size
        virtual void set(size_t i, T* obj) {
            if (i < len_) {
                // NOTE: user must delete replaced object before set or still have reference to it. 
                values_[i] = obj;  // do not copy on add
            } else {
                push(obj);
            }
        }

        // deletes the contents of the array
        virtual void clear() {
            len_ = 0;
        }

        // returns the first index of a given object, or -1 if the array does not contain the object
        virtual int indexOf(T* obj) {
            return indexOf(obj, 0);
        }

        // returns the first index of a given object starting at index i, or -1 if the array does not 
        // contain the object after index i
        virtual int indexOf(T* obj, size_t i) {
            for (size_t idx = i; idx < len_; idx++) {
                if (values_[idx]->equals(obj)) {
                    return idx;
                }
            }
            return -1;
        }

        // returns an array with elements of this array combined with elements of other
        virtual Array* concat(Array* other) {
            Array* ret = new Array(size() + other->size());
            for (size_t i = 0; i < len_; i++) {
                ret->push(values_[i]);
            }
            for (size_t i = 0; i < other->size(); i++) {
                ret->push(other->get(i));
            }
            return ret;
        }

        // returns if this array has the same values at the other array
        virtual bool equals(Object* other) {
            Array* o = dynamic_cast<Array*>(other);
            if (o == nullptr || o->size() != size()) {
                return false;
            }
            for (size_t i = 0; i < len_; i++) {
                if (!values_[i]->equals(o->get(i))) {
                    return false;
                }
            }
            return true;
        }

        // returns the hash of the array
        virtual size_t hash() {
            size_t ret = 0;
            for (size_t i = 0; i < len_; i++) {
                ret = ret << 6;
                ret ^= values_[i]->hash();
            }
            return ret;
        }

        // returns the size of the array
        virtual size_t size() {
            return len_;
        }

        // removes and returns the object at index i
        // This is our own function
        // @return: nullptr on i > size else object at i
        virtual T* remove(size_t i) {
            if (i >= len_) {
                return nullptr;
            }

            T *ret = values_[i];
            for (size_t j = i; j < len_ - 1; j++) {
                values_[j] = values_[j+1];
            }

            len_--;

            return ret;
        }
};