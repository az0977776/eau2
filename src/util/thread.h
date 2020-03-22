// COPIED FROM CLASS NOTES, CREDIT TO JAN
#pragma once
// lang::Cpp
#include <cstdlib>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <sstream>
#include "object.h"
#include "string.h"

/** A Thread wraps the thread operations in the standard library.
 *  author: vitekj@me.com */
class Thread : public Object {
public:
    std::thread thread_;

    /** Starts running the thread, invoked the run() method. */
    void start() { thread_ = std::thread([this]{ this->run(); }); }

    /** Wait on this thread to terminate. */
    void join() { thread_.join(); }

    /** Yield execution to another thread. */
    static void yield() { std::this_thread::yield(); }

    /** Sleep for millis milliseconds. */
    static void sleep(size_t millis) {
       std::this_thread::sleep_for(std::chrono::milliseconds(millis));
    }

    /** Subclass responsibility, the body of the run method */
    virtual void run() { assert(false); }

    // there's a better way to get an CwC value out of a threadid, but this'll do for now
    /** Return the id of the current thread */
    static String * thread_id() {
        std::stringstream buf;
        buf << std::this_thread::get_id();
        std::string buffer(buf.str());
        return new String(buffer.c_str(), buffer.size());
    }
};

static unsigned int get_thread_count() {
    return std::thread::hardware_concurrency(); 
}