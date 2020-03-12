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
        size_t second_int_col_ = 5;

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
            df_->set(second_int_col_, r.get_idx(), result);
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

/**
 * Product calculates the product of all floats and ints in the row, then sets the last
 * float column with the value and keeps a running product.
 */
class Product : public Rower {  
    public:
        DataFrame* df_;
        double product_;

        size_t first_int_col_ = 1;
        size_t first_float_col_ = 2;
        size_t second_int_col_ = 5;
        size_t second_float_col_ = 6;
        size_t third_int_col_ = 9;

        Product(DataFrame* df) {
            product_ = 0;
            df_ = df;
        }

        /**
         * comuptes the product of all int and float columns. Updates this rowers product value
         * and sets the last float column with the value. 
         * NOTE: product may overflow
         */
        bool accept(Row& r) {
            float result = r.get_int(first_int_col_) * r.get_float(first_float_col_) \
                        * r.get_int(second_int_col_) * r.get_float(second_float_col_) \
                        * r.get_int(third_int_col_);
            product_ *= result;
            df_->set(second_float_col_, r.get_idx(), result);

            return true;
        }

        Object* clone() {
            return new Product(df_);
        }

        // deletes the other rower, multiplies the other Rower's product to this product
        void join_delete(Rower* other)  {
            Product *s = dynamic_cast<Product *>(other);
            if (s == nullptr) {
                delete other;
                return;
            }
            product_ *= s->product_;
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

