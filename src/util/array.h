//lang::CwC
#pragma once

#include "object.h"
#include <stdlib.h>

// @author: Chris Barth <barth.c@husky.neu.edu> and Aaron Wang <wang.aa@husky.neu.edu>
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

        virtual void push_back(T* obj) {
            push(obj);
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