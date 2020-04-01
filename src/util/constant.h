#pragma once
#include <unistd.h>

// global vars
static const size_t MAX_SIZE_T = -1;
static const size_t ARRAY_STARTING_CAP = 4;

// column.h
static const size_t CHUNK_SIZE = sizeof(size_t) * 128;  // on 64-bit machine this is 8 * 128 = 1024

// sorer.h
static const int BUFF_LEN = 4096 * 16;      // max line length
static const int INFER_LINE_COUNT = 500;    // number of lines to infer from

// keyvaluestore.h
static const char* SERVER_IP = "127.0.0.1";     // ip address of the server
// TODO: make it easier to change this for each application
static const char* CLIENT_IP = "127.0.0.1";     // ip address of each client

// network.h
static const int SERVER_LISTEN_PORT = 8080;     // port that the server listens on
static const int CLIENT_NUM = 3;                // maximum number of clients
static const int MAX_PACKET_LENGTH = 1024;      // The maximum number of bytes in a package
