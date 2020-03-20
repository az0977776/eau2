//lang:cpp
#pragma once
#include <stdlib.h>
#include "object.h"

// does not own T added to the vector
template <class T>
class Vector : public Object {
    public:
        size_t len_, cap_;
        T* data_;  // own array but not data

        Vector() {
            cap_ = 4;
            len_ = 0;
            data_ = new T[cap_];
        }  

        ~Vector() {
            delete[] data_;
        }

        void check_and_reallocate_() {
            if (len_ >= cap_) {
                cap_ *=2;
                T* temp = new T[cap_];
                for (size_t i = 0; i < len_; i++) {
                    temp[i] = data_[i];
                }
                delete[] data_;
                data_ = temp;
            }
        }

        void push_back(T val) {
            check_and_reallocate_();
            data_[len_++] = val;
        }

        size_t size() {
            return len_;
        }
        
        T get(size_t idx) {
            if (idx < size()) {
                return data_[idx];
            }
            return nullptr;
        }

        void set(size_t idx, T val) {
            if (idx < size()) {
                data_[idx] = val;
            } else {
                push_back(val);
            }
        }
        
        T remove(size_t idx) {
            T temp = get(idx);
            
            for (size_t i = idx; i < size() - 1; i++) {
                data_[i] = data_[i+1];
            }
            return temp;
        }
};