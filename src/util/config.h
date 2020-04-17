#pragma once
#include <unistd.h>
#include "object.h"

class Config : public Object {
    public:
        // global vars
        static const size_t MAX_SIZE_T = -1;
        static const size_t ARRAY_STARTING_CAP = 4;

        // sorer.h
        static const int BUFF_LEN = 4096 * 16;          // max line length
        static const int INFER_LINE_COUNT = 500;        // number of lines to infer from

        // network.h
        static const int SERVER_LISTEN_PORT = 8080;     // port that the server listens on
        static const int MAX_PACKET_LENGTH = 1024;      // The maximum number of bytes in a package

        // configuarable values
        size_t CLIENT_NUM = 3;                          // maximum number of clients
        char* CLIENT_IP;                                // ip address of each client
        char* SERVER_IP;                                // ip address of the server
        size_t CHUNK_SIZE = 1024;                       // how many elements per chunk in column
        size_t SERVER_UP_TIME = 20;                     // how long the server stays online for
        
        Config() {
            FILE* file = fopen("config.txt", "r");
            abort_if_not(file != NULL, "config file is null pointer");
            char buff[1024];
            char* field;
            char* value;
            CLIENT_IP = new char[16];
            memset(CLIENT_IP, 0, 16);

            SERVER_IP = new char[16];
            memset(SERVER_IP, 0, 16);

            while(fgets(buff, 1024, file)) {
                field = strtok(buff, "=");
                value = strtok(NULL, "\n# ");  // this will get the value up to '#', ' ', or '\n' 

                // printf("CONFIG -- field: %s, value: %s\n", field, value);

                if (strcmp(field, "CLIENT_NUM") == 0) {
                    CLIENT_NUM = atoi(value);
                }
                else if (strcmp(field, "CLIENT_IP") == 0) {
                    memcpy(CLIENT_IP, value, strlen(value) + 1);
                }
                else if (strcmp(field, "CHUNK_SIZE") == 0) {
                    CHUNK_SIZE = atoi(value);
                }
                else if (strcmp(field, "SERVER_UP_TIME") == 0) {
                    SERVER_UP_TIME = atoi(value);
                }
                else if (strcmp(field, "SERVER_IP") == 0) {
                    memcpy(SERVER_IP, value, strlen(value) + 1);
                }
            }
            
            fclose(file);
        }

        ~Config() {
            delete[] CLIENT_IP;
            delete[] SERVER_IP;
        }
};

