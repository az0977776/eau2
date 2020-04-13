#include <stdlib.h>

#include "../dataframe/dataframe.h"
#include "../kvstore/keyvalue.h"
#include "application.h"

static const size_t SZ = 100 * 1000;

class Demo : public Application {
  public:
    Key main;
    Key verify;
    Key check;
  
    Demo() : Application(), main(0, "main"), verify(0, "verif"), check(0, "check") { }
  
    void run_() override {
      switch(this_node()) {
      case 0:   producer();     break;
      case 1:   counter();      break;
      case 2:   summarizer();
      }
    }
  
    void producer() {
      // printf("This is the producer node with node idx: %zu\n", this_node());
      double* vals = new double[SZ];
      double sum = 0;
      for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
      DataFrame::fromArray(&main, &kv, SZ, vals);
      DataFrame::fromScalar(&check, &kv, sum);
      // printf("Producer is done\n");
    }
  
    void counter() {
      // printf("This is the counter node with node idx: %zu\n", this_node());
      DataFrame* v = getAndWait(main);
      double sum = 0;
      for (size_t i = 0; i < SZ; ++i) {
        sum += v->get_double(0,i);
      }
      p("The sum is  ").pln(sum);
      DataFrame::fromScalar(&verify, &kv, sum);
      // printf("Counter is done\n");
    }
  
    void summarizer() {      
      // printf("This is the summarizer node with node idx: %zu\n", this_node());
      DataFrame* result = getAndWait(verify);
      DataFrame* expected = getAndWait(check);
      pln(expected->get_double(0,0)==result->get_double(0,0) ? "Milestone3: SUCCESS":"Milestone3: FAILURE");
      // printf("Summarizer is done\n");
    }
};
