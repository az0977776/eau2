#pragma once
//lang::Cpp

#include <cstdlib>
#include <cstring>
#include <iostream>

#include <errno.h>


#include <stdarg.h> /* va_end(), va_list, va_start(), vprintf() */
#include <stdio.h> /* vprintf() */

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
  Sys& pln(double x) { std::cout << x << "\n";  return *this; }
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

  // if not b then print the exit message with a format string and exit with code -1
  void abort_if_not(bool b, const char* fmt, ...) {
      if (b) return;
      printf("Exit message: \"");
      va_list ap;
      va_start(ap, fmt);
      vprintf(fmt, ap);
      va_end(ap);
      printf("\" -- errno: %d\n", errno);
      // printf("\"\n");
      exit(-1);
  }

  // Print the exit message and exit with code -1
  static void fail(const char* fmt, ...) {
    printf("Fail message: \"");
    va_list ap;
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\"\n");
    exit(-1);
  }

  // Try to print each byte in the buf as a char, if it is a special char print as hex
  // Used in debugging
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
};
