#pragma once
//lang::Cpp

#include <cstdlib>
#include <cstring>
#include <iostream>

#include <errno.h>

/** Helper class providing some C++ functionality and convenience
 *  functions. This class has no data, constructors, destructors or
 *  virtual functions. Inheriting from it is zero cost.
 */
class Sys {
 public:

  // Printing functions
  Sys& p(char* c) { std::cout << c; return *this; }
  Sys& p(bool c) { std::cout << c; return *this; }
  Sys& p(float c) { std::cout << c; return *this; }
  Sys& p(double c) { std::cout << c; return *this; }  
  Sys& p(int i) { std::cout << i;  return *this; }
  Sys& p(size_t i) { std::cout << i;  return *this; }
  Sys& p(const char* c) { std::cout << c;  return *this; }
  Sys& p(char c) { std::cout << c;  return *this; }
  Sys& pln() { std::cout << "\n";  return *this; }
  Sys& pln(int i) { std::cout << i << "\n";  return *this; }
  Sys& pln(char* c) { std::cout << c << "\n";  return *this; }
  Sys& pln(bool c) { std::cout << c << "\n";  return *this; }  
  Sys& pln(char c) { std::cout << c << "\n";  return *this; }
  Sys& pln(float x) { std::cout << x << "\n";  return *this; }
  Sys& pln(size_t x) { std::cout << x << "\n";  return *this; }
  Sys& pln(const char* c) { std::cout << c << "\n";  return *this; }

  // Copying strings
  char* duplicate(const char* s) {
    char* res = new char[strlen(s) + 1];
    strcpy(res, s);
    return res;
  }
  char* duplicate(char* s) {
    char* res = new char[strlen(s) + 1];
    strcpy(res, s);
    return res;
  }

  // Function to terminate execution with a message
  void exit_if_not(bool b, char* c) {
    if (b) return;
    p("Exit message: ").pln(c);
    exit(-1);
  }

  // alternative to exit_if_not but takes in a const char* instead of a char*
  void abort_if_not(bool b, const char* c) {
      if (b) return;
      std::cout << "Exit message: \"" << c << "\" -- errno: " << errno << "\n";
      exit(-1);
  }

  static void fail(const char* c) {
    std::cout << "Fail message: " << c << "\n";
    exit(-1);
  }

  static void print_byte(const char* buf, size_t len) {
      for (size_t i = 0; i < len; i++) {
          if (buf[i] < 32 || buf[i] > 126) {
            printf("[0x%02X] ", (unsigned char) buf[i]);
          } else {
            printf("[%c] ", buf[i]);
          }
      }
      printf("\n");
  }
  
// Definitely fail
//  void FAIL() {
  void myfail(){
    pln("Failing");
    exit(1);
  }

  // Some utilities for lightweight testing
  void OK(const char* m) { pln(m); }
  void t_true(bool p) { if (!p) myfail(); }
  void t_false(bool p) { if (p) myfail(); }

  void affirm(bool test, const char* msg) {
      if (!(test)){ 
          fprintf(stderr, "%s\n", msg); 
          abort(); 
      }
  }

  void check_in_bounds(int argc, int index, int arguments) {
      affirm((index + arguments < argc), "Missing argument");
  }

  unsigned int check_positive(int i) {
      affirm((i >= 0), "Only positive integers allowed");
      return i;
  }
};
