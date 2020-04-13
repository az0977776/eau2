//lang:CwC
#include <unistd.h>
#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "kvstore/network.h"
#include "util/thread.h"

// The server defaults to the local machine's ip address, ip addresses passed will be ignored
int main() {
    assert(get_thread_count() > 1);
    Config config;
    Server* s = new Server("127.0.0.1");

    // char buf[1024]; 
    // while(fgets(buf, 1024, stdin)) {
    //     printf("Enter \"exit\" to quit: ");

    //     if (strlen(buf) == 0 || buf[0] == '\n') {
    //         continue;
    //     }; 
    //     buf[strlen(buf) - 1] = '\0';

    //     if (strcmp(buf, "exit") == 0) {
    //         printf("Exiting\n");
    //         break;
    //     }
    // }

    sleep(config.SERVER_UP_TIME);
    
    delete s;
    return 0;
}