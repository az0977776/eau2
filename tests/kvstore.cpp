//lang:CwC
#include <unistd.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <signal.h>
#include <unistd.h>

#include "../src/kvstore/network.h"
#include "../src/kvstore/keyvalue.h"
#include "../src/kvstore/keyvaluestore.h"

#include "../src/util/serial.h"


int main(int argc, char** argv) {
    KVStore kvs;  // starts the clinet

    // parent
    char buf[1024]; 
    while(true) {
        printf("Enter a command or type \"help\": ");
        assert(fgets(buf, 1024, stdin));
        if (strlen(buf) == 0 || buf[0] == '\n') {
            continue;
        }; 
        buf[strlen(buf) - 1] = '\0';

        if (strcmp(buf, "exit") == 0) {
            printf("Exiting\n");
            break;
        }
        else if (strcmp(buf, "help") == 0) {
            printf("send <ip> <port> <message>\nexit\nhelp\n");
        }
        else {
            char * pch;
            char *ip;
            char *key;
            char *value;
            int port;
            char *msg;
            pch = strtok (buf, " ");
            if (strcmp(pch, "get") == 0) {
                ip = strtok(NULL, " ");
                msg = strtok(NULL, "\n");
                Key k(atoi(ip), msg);
                Value* rv = kvs.get(k);
                if (rv == nullptr) {
                    printf("did not get a return message\n");
                } else {
                    printf("Got a return message: ");
                    rv->print();
                    printf("\n");
                    delete rv;
                }
            } else if (strcmp(pch, "getandwait") == 0) {
                ip = strtok(NULL, " ");
                msg = strtok(NULL, "\n");
                Key k(atoi(ip), msg);
                Value* rv = kvs.getAndWait(k);
                if (rv == nullptr) {
                    printf("did not get a return message\n");
                } else {
                    printf("Got a return message: ");
                    rv->print();
                    printf("\n");
                    delete rv;
                }
            } else if (strcmp(pch, "put") == 0) {
                ip = strtok(NULL, " ");
                key = strtok(NULL, " ");
                value = strtok(NULL, "\n");

                Key k(atoi(ip), key);
                Value v(strlen(value) + 1, value);

                kvs.put(k, v);
                printf("did a put command\n");
            } else {
                printf("Invalid command\n");
                continue;
            }
        }
    }

    return 0;
}
