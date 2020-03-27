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
#include "../util/serial.h"
#include "keyvalue.h"

// this is a class that has a callback method to be called when a message is returned
class ClientMessageHandler : public MessageHandler {
    public:
        ClientMessageHandler() { }

        ~ClientMessageHandler() { }
    
        // handle a genaric message coming from the given sender
        // return: the response the the given message.
        //          if respnse is nullptr then nothing is sent back.
        virtual Response* handle_message(sockaddr_in server, size_t data_len, char* data) { 
            printf("Got a Message (len = %zu): ", data_len);
            print_byte(data, data_len);
            return nullptr;
        }

        // handle a genaric message coming from the given sender
        // return: the response the the given message.
        //          if respnse is nullptr then nothing is sent back.
        virtual Response* handle_get(sockaddr_in server, size_t data_len, char* data) {
            printf("Got a Get Message (len = %zu): ", data_len);
            print_byte(data, data_len);

            return new Response(server, 16, "Response to get");
        }

        // handle a genaric message coming from the given sender
        // return: the response the the given message.
        //          if respnse is nullptr then nothing is sent back.
        virtual Response* handle_get_and_wait(sockaddr_in server, size_t data_len, char* data) {
            printf("Got a Get And Wait Message (len = %zu): ", data_len);
            print_byte(data, data_len);
            return nullptr;
        }
        
        // handle a genaric message coming from the given sender
        // return: the response the the given message.
        //          if respnse is nullptr then nothing is sent back.
        virtual Response* handle_put(sockaddr_in server, size_t data_len, char* data) {
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
            char *key;
            char *value;
            int port;
            char *msg;
            pch = strtok (buf, " ");
            if (strcmp(pch, "send") == 0) {
                ip = strtok(NULL, " ");
                port = atoi(strtok(NULL, " "));
                msg = strtok(NULL, "\n");
                c->send_string(ip, port, msg);

                /*
                    // Getting a value associated with a key on another key value store client
                    char* get(size_t to_node_idx, size_t payload_len, char* payload, size_t &return_msg_len) {
                        Get message(get_sockaddr(), payload_len, payload);
                        
                        return send_to_node_(to_node_idx, &message, return_msg_len);
                    }
                */
            } else if (strcmp(pch, "get") == 0) {
                ip = strtok(NULL, " ");
                msg = strtok(NULL, "\n");
                Key k(atoi(ip), msg);
                size_t resp_size;
                char* rv = c->get(atoi(ip), k.serial_buf_size(), k.serialize(), resp_size);
                if (rv == nullptr) {
                    printf("did not get a return message\n");
                } else {
                    printf("Got a return message of size %zu: %s\n", resp_size, rv);
                    delete rv;
                }
                /*
                    void put(size_t to_node_idx, size_t payload_len, char* payload) {
                        Put message(get_sockaddr(), payload_len, payload);
                        size_t return_msg_len = 0;
                        
                        char* rv = send_to_node_(to_node_idx, &message, return_msg_len);
                        abort_if_not(return_msg_len == 0 && rv == nullptr, "Got a response from a put message");
                    }
                */
            } else if (strcmp(pch, "put") == 0) {
                ip = strtok(NULL, " ");
                key = strtok(NULL, " ");
                value = strtok(NULL, "\n");

                Key k(atoi(ip), key);
                Value v(strlen(value) + 1, value);

                size_t buf_len = k.serial_buf_size() + v.size();
                char* buf = new char[buf_len];
                k.serialize(buf);
                memcpy(buf + k.serial_buf_size(), v.get(), v.size());

                c->put(atoi(ip), buf_len, buf);
                printf("did a put command\n");
            } else {
                printf("Invalid command\n");
                continue;
            }
        }
    }

    delete c;
    return 0;
}
