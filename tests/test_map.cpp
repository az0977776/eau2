//lang:CwC
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>

#include "../src/dataframe.h"
#include "../src/sorer.h"

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

        Fibonacci(DataFrame* df) {
            df_ = df;
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
            int result = fibonacci(r.get_int(first_int_col_) % modulo_); // ensure that fib is calculated from small values 
            df_->set(first_int_col_, r.get_idx(), result);
            return true;
        }

        Object* clone() {
            return new Fibonacci(df_);
        }

        // deletes the other rower
        void join_delete(Rower* other)  {
            delete other;
        }
};

int main() {

    SOR sorer("../data/data.sor");

    DataFrame* df = sorer.read();
    
    Fibonacci fib(df);

    df->pmap(fib);

    delete df;

    printf("OK: test_map\n");
    return 0;
}

