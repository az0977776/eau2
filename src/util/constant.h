#pragma once
#include <unistd.h>

// column.h
static const size_t ARRAY_STARTING_CAP = 4;
static const size_t CHUNK_SIZE = sizeof(size_t) * 128;  // on 64-bit machine this is 8 * 128 = 1024

// sorer.h
static const int buff_len = 4096 * 16; 
static const int infer_line_count = 500;

// keyvaluestore.h
static const char* SERVER_IP = "127.0.0.1";
static const char* CLIENT_IP = "127.0.0.1";

// network.h
static const int SERVER_LISTEN_PORT = 8080;
static const int CLIENT_NUM = 5;
static const int BUFF_LEN = 1024;
static const size_t MAX_SIZE_T = -1;
