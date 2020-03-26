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

#include "network.h"

// this is a class that has a callback method to be called when a message is returned
class ClientMessageHandler : public MessageHandler {
    public:
        ClientMessageHandler() { }

        ~ClientMessageHandler() { }
    
        // handle a genaric message coming from the given sender
        // return: the response the the given message.
        //          if respnse is nullptr then nothing is sent back.
        virtual Response* handle_message(String &sender, size_t data_len, char* data) { 
            printf("Got a Message (len = %zu): ", data_len);
            print_byte(data, data_len);
            return nullptr;
        }

        // handle a genaric message coming from the given sender
        // return: the response the the given message.
        //          if respnse is nullptr then nothing is sent back.
        virtual Response* handle_get(String &sender, size_t data_len, char* data) {
            printf("Got a Get Message (len = %zu): ", data_len);
            print_byte(data, data_len);
            return nullptr;
        }

        // handle a genaric message coming from the given sender
        // return: the response the the given message.
        //          if respnse is nullptr then nothing is sent back.
        virtual Response* handle_get_and_wait(String &sender, size_t data_len, char* data) {
            printf("Got a Get And Wait Message (len = %zu): ", data_len);
            print_byte(data, data_len);
            return nullptr;
        }
        
        // handle a genaric message coming from the given sender
        // return: the response the the given message.
        //          if respnse is nullptr then nothing is sent back.
        virtual Response* handle_put(String &sender, size_t data_len, char* data) {
            printf("Got a Put Message (len = %zu): ", data_len);
            print_byte(data, data_len);
            return nullptr;
        }
};


int main(int argc, char** argv) {
    const char *ip;
    const char *default_ip = "127.0.0.1";
    int port = 9000;
    bool a_ip = false;
    bool a_port = false;

    int index = 1; // skipping the first argument
    while (index < argc) {
        if (strcmp(argv[index], "-ip") == 0) {
            ip = argv[index + 1];
            a_ip = true;
            index+=2;
        }
        else if (strcmp(argv[index], "-port") == 0) {
            port = atoi(argv[index + 1]);
            a_port = true;
            index+=2;
        }
        else {
            printf("Invalid input argument\n");
            exit(1);
        }
    }

    if (!a_ip) {
        ip = default_ip;
        printf("defaulting ip to 127.0.0.1\n");
    }
    if (!a_port) {
        printf("defaulting port to 9000\n");
    }

    ClientMessageHandler msg_handler;
    Client *c = new Client(ip, "127.0.0.1", &msg_handler);

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
            int port;
            char *msg;
            pch = strtok (buf, " ");
            if (strcmp(pch, "send") != 0) {
                printf("Invalid command\n");
                continue;
            }
            ip = strtok(NULL, " ");
            port = atoi(strtok(NULL, " "));
            msg = strtok(NULL, "\n");
            c->send_string(ip, port, msg);
        }
    }

    delete c;
    return 0;
}
