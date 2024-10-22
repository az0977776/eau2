//lang:CwC
#pragma once

#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>

#include "../../src/dataframe/dataframe.h"
#include "../../src/dataframe/sorer.h"

/**
 * This is a Dataframe Rower that will compute the fibonacci number based on
 * the modulo of the first integer column. A naive implementation of fibonacci
 * is used to increase runtime.
 */
class Fibonacci : public Rower {  
    public:
        DataFrame* df_;

        size_t modulo_ = 22;
        size_t first_int_col_ = 1;

        size_t sum_;

        Fibonacci(DataFrame* df) {
            df_ = df;
            sum_ = 0;
        }

        /**
         * A naive implementation of the fibonacci numbers. 
         * @return: the nth fibonacci number where 0th = 0 and 1st = 1
         */
        int fibonacci(int n) {
            // 0 1 1 2 3 5 ...
            if (n < 2) {
                return n;
            } else {
                return fibonacci( n - 1 ) + fibonacci(n - 2);
            }
        }

        /**
         * Fibonacci is calculated and then set into the dataframe. 
         * This will get the modulus of the first integer in the row, to
         * reduce the possible number of fibonacci numbers needed to calculate
         */
        bool accept(Row& r) {
            sum_ += fibonacci(r.get_int(first_int_col_) % modulo_); // ensure that fib is calculated from small values 
            return true;
        }

        Object* clone() {
            return new Fibonacci(df_);
        }

        size_t get_sum() {
            return sum_;
        }

        // deletes the other rower
        void join_delete(Rower* other)  {
            Fibonacci* f = dynamic_cast<Fibonacci*>(other);
            abort_if_not(f != nullptr, "Fibonacci: cast failure\n");
            sum_ += f->get_sum();
            delete other;
        }
};

void run_map_tests() {
    KVStore kvs(false);
    SOR sorer("../../data/data.sor", &kvs);

    DataFrame* df = sorer.read();
    
    Fibonacci fib(df);

    df->pmap(fib);

    assert(fib.get_sum() == 146);

    delete df;

    printf("OK: test_map\n");
}

