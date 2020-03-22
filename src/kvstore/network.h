//lang:CwC
#pragma once
#include <unistd.h>
#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <signal.h>
#include <sys/time.h>
#include <pthread.h>

#include "../util/helper.h"
#include "../util/object.h"
#include "../util/string.h"
#include "../util/serial.h"
#include "../util/thread.h"

static const int SERVER_LISTEN_PORT = 8080;
static const int CLIENT_NUM = 5;
static const int BUFF_LEN = 1024;

// to be implemented from
// this is a class that has a callback method to be called when a message is returned
class MessageHandler : public Object {
    public:
    
        // handle the data coming from the given sender
        virtual void handle_message_data(String &sender, const char* data) { }

        // overwrite to change behavior when recieving a directory. 
        // All clients are comma separated in the form <ip>:<port>
        virtual void handle_new_directory(String &clients) { }
};

// Network is a subclass of Object
// Network abstracts creating a listening socket, accepting connections, 
// connecting to a remote socket and sending messages to sockets.
class Network : public Object {
    public:
        bool quitting_; // is this network object quitting

        // sends a char* to the given file descriptor
        // returns true if all characters were sent, false otherwise
        bool send_chars(int fd, size_t num_bytes, const char *c) {
            return send(fd, c, num_bytes, 0) == num_bytes;
        }

        // sends the payload from the given header as packets of at most BUFF_LEN
        void send_payload_(int fd, Header &header) {
            char* buf = header.serialize();

            for (size_t i = 0; i < header.get_payload_size(); i += BUFF_LEN) {
                size_t packet_size = header.get_payload_size() - i < BUFF_LEN? header.get_payload_size() - i : BUFF_LEN;
                abort_if_not(send_chars(fd, packet_size, buf + i), "failed to send a packet payload");
            }
            delete[] buf;
        }

        // Sends the given header over the given file descriptor
        void send_message(int fd, Header &header) {
            char* buf = new char[HEADER_SIZE];
            Header* check_header2;

            // sending the header to the file descriptor
            abort_if_not(send_chars(fd, header.header_len(), header.get_header()), "Failed to send header");

            read(fd, buf, header.header_len());
            Header check_header(buf);

            // check that the returned header type
            switch (check_header.get_type()) {
                case MsgKind::ACK:
                    // ACK means protocall is finished
                    break;
                case MsgKind::READY:
                    // the reciever is ready for the data so send
                    send_payload_(fd, header);
                    // check what is returned
                    read(fd, buf, header.header_len());
                    check_header2 = new Header(buf);
                    
                    // returned header should be an ACK to close the connection
                    abort_if_not(check_header2->get_type() == MsgKind::ACK, "No 'ACK' to payload");
                    delete check_header2;
                    break;
                default:
                    // any other type at this time is an error
                    abort_if_not(false, "Did not get a 'ready' or 'ack' message back after sending header");
                    break;
            }
            delete[] buf;
        }

        // Recieve the a payload of the given size over the given file descriptor. This will recieve the payload in 
        // packets of size BUFF_LEN or less. Returns a buffer of the given size.
        char* receive_payload_(int fd, size_t payload_size) {
            char* payload = new char[payload_size];

            for (size_t i = 0; i < payload_size; i += BUFF_LEN) {
                size_t packet_size = payload_size - i < BUFF_LEN? payload_size - i : BUFF_LEN;
                abort_if_not(read(fd, payload + i, packet_size), "failed to receive a packet payload");
            }
            return payload;
        }

        // This is a method to be overridden. Gets the sockaddr_in that represents this network object
        virtual sockaddr_in get_sockaddr() {
            return { 0 };
        }

        Header* recieve_message(int fd) {
            Header* rv = nullptr;
            char* payload = nullptr;
            char* buf = new char[HEADER_SIZE];
            Ready ready(get_sockaddr());
            Ack ack(get_sockaddr());

            // read in the header of a message
            abort_if_not(read(fd, buf, HEADER_SIZE), "Failed to receive message header");
            Header check_header(buf);

            // check what the message is
            switch(check_header.get_type()) {
                case MsgKind::REGISTER:
                    // send the ready to recieve message and start reading from the file descriptor
                    abort_if_not(send_chars(fd, ready.header_len(), ready.get_header()), "Register: Failed to send ready header");
                    payload = receive_payload_(fd, check_header.get_payload_size());

                    // deserialize the payload into a register message. Set up return value.
                    rv = new Register(check_header.get_sender(), payload);
                    delete payload;

                    // Send an ack to close the connection
                    abort_if_not(send_chars(fd, ack.header_len(), ack.get_header()), "Register: Failed to send ack header");
                    break;
                case MsgKind::DEREGISTER:
                    // send the ready to recieve message and start reading from the file descriptor
                    abort_if_not(send_chars(fd, ready.header_len(), ready.get_header()), "Deregister: Failed to send ready header");
                    payload = receive_payload_(fd, check_header.get_payload_size());
                    
                    // deserialize the payload into a register message. Set up return value.
                    rv = new Deregister(check_header.get_sender(), payload);
                    delete payload;
                    
                    // Send an ack to close the connection
                    abort_if_not(send_chars(fd, ack.header_len(), ack.get_header()), "Deregister: Failed to send ack header");
                    break;
                case MsgKind::DIRECTORY:
                    // send the ready to recieve message and start reading from the file descriptor
                    abort_if_not(send_chars(fd, ready.header_len(), ready.get_header()), "Directory: Failed to send ready header");
                    payload = receive_payload_(fd, check_header.get_payload_size());

                    // deserialize the payload into a register message. Set up return value.
                    rv = new Directory(check_header.get_sender(), payload);
                    delete payload;

                    // Send an ack to close the connection
                    abort_if_not(send_chars(fd, ack.header_len(), ack.get_header()), "Directory: Failed to send ack header");
                    break;
                case MsgKind::MESSAGE:
                    // send the ready to recieve message and start reading from the file descriptor
                    abort_if_not(send_chars(fd, ready.header_len(), ready.get_header()), "Message: Failed to send ready header");
                    payload = receive_payload_(fd, check_header.get_payload_size());

                    // deserialize the payload into a register message. Set up return value.
                    rv = new Message(check_header.get_sender(), check_header.get_payload_size(), payload);
                    delete payload;

                    // Send an ack to close the connection
                    abort_if_not(send_chars(fd, ack.header_len(), ack.get_header()), "Message: Failed to send ack header");
                    break;
                case MsgKind::SHUTDOWN:
                    // Send an ack to close the connection
                    abort_if_not(send_chars(fd, ack.header_len(), ack.get_header()), "Shutdown: Failed to send ack header");
                    // set up return value with shutdown command
                    rv = new Shutdown(check_header.get_sender());
                    break;
                default:
                    abort_if_not(false, "Received a bad message");
                    break;
            }
            delete[] buf;
            return rv;
        }

        // binds a socket to the given listen port and starts listening
        // returns the file descriptor for the socket
        int get_listen_socket(const char* listen_ip, int listen_port) {
            struct sockaddr_in adr;
            int opt = 1;
            int ret_fd;
            abort_if_not((ret_fd = socket(AF_INET, SOCK_STREAM, 0)) != 0, "get_listen_socket(): failed to create socket");
            // attaching socket to the listen ip and port
            abort_if_not(setsockopt(ret_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt)) != 0, "get_listen_socket(): failed to set socket options");
            adr.sin_family = AF_INET;
            abort_if_not(inet_pton(AF_INET, listen_ip, &adr.sin_addr) > 0, "get_listen_socket(): failed to convert string ip address to bytes");
            adr.sin_port = htons( listen_port ); // uses the listen port
            abort_if_not(bind(ret_fd, (struct sockaddr *)&adr, sizeof(adr))>=0, "get_listen_socket(): failed to bind on the file descriptor");
            abort_if_not(listen(ret_fd, CLIENT_NUM) >= 0, "get_listen_socket(): failed to listen to file descriptor");
            return ret_fd;
        }

        // to be inherited and overwritten
        // handles a message
        virtual void handle_message(Header* message) {
            return;
        }

        // to be inherited and overwritten
        // this accepts connections on the default listening file descriptor
        virtual void accept_connections() { }

        // continues to accept and handle connections on the given file descriptor
        void accept_connections(int fd) {
            Header* message = nullptr;
            struct sockaddr_storage client_addr;
            socklen_t addr_size = sizeof(client_addr);
            int new_fd;
            struct timeval tv;
            fd_set readfds;

            tv.tv_sec = 1;
            tv.tv_usec = 0;

            while (true) {
                // reset the read file descriptors
                FD_ZERO(&readfds);
                FD_SET(fd, &readfds);

                // timeout or recieve a connection
                select(fd + 1, &readfds, NULL, NULL, &tv);

                if (FD_ISSET(fd, &readfds)) {
                    // accept the connection
                    new_fd = accept(fd, (struct sockaddr *)&client_addr, &addr_size);
                    abort_if_not(new_fd != -1, "Accept connection: failed to accept connection");
                    message = recieve_message(new_fd);
                    handle_message(message);
                    delete message;
                    message = nullptr;
                    close(new_fd);
                }
                else if (quitting_) {
                    printf("Listening server is quitting\n");
                    break;
                }
            }
        }

        // connect to a given ip and port
        int connect_to(const char* ip, unsigned short port) {
            struct sockaddr_in ipp;
            ipp.sin_port = port;
            abort_if_not(inet_pton(AF_INET, ip, &ipp.sin_addr)>0, "connect_to: failed to convert ip address string to bytes");
            return connect_to(&ipp);
        }

        // connect to a given sockaddr_in which holds an ip and a port
        int connect_to(struct sockaddr_in *ipp) {
            struct sockaddr_in remote;
            int remote_sock;
            abort_if_not((remote_sock = socket(AF_INET, SOCK_STREAM, 0)) >= 0, "connect_to: failed to create a socket");
            remote.sin_family = AF_INET; // ipv4
            remote.sin_port = htons(ipp->sin_port); // use the given ip
            remote.sin_addr = ipp->sin_addr; // use the given port
            abort_if_not(connect(remote_sock, (struct sockaddr *)&remote, sizeof(remote)) >= 0, "connect_to: failed to connect on the given file descriptor");
            return remote_sock;
        }
};

// This is a thread that will accept connections for the given network object.
class ListeningThread : public Thread {
    public:
        Network* network_;  // does not own

        ListeningThread(Network* network) {
            network_ = network;

        }

        ~ListeningThread() { }

        void run() {
            network_->accept_connections();
        }

};

// Server is a subclass of Network
// the server handles client registration and can return a list of all registered
// clients back to clients
class Server : public Network {
    public:
        ListeningThread* listening_thread_;  // owens
        int server_fd_;
        const char* server_ip_;


        pthread_mutex_t lock_;  // this locks both the array and the size t for both reads and writes
        struct sockaddr_in client_info_[CLIENT_NUM];
        size_t client_count_;

        Server(const char* server_ip) {
            abort_if_not(pthread_mutex_init(&lock_, NULL) == 0, "Server: Failed to create mutex");
            quitting_ = false;
            server_ip_ = server_ip;
            client_count_ = 0;
            server_fd_ = get_listen_socket(server_ip, SERVER_LISTEN_PORT);

            listening_thread_ = new ListeningThread(this);
            listening_thread_->start();
        }

        ~Server() {
            printf("Server shutting down.\n");
            shutdown_clients();

            // set the quitting flag and then wait for the listening thead to finish
            quitting_ = true;
            listening_thread_->join();

            delete listening_thread_;
            close(server_fd_);

            pthread_mutex_destroy(&lock_); 
        }

        virtual sockaddr_in get_sockaddr() {
            struct sockaddr_in addr;
            addr.sin_family = AF_INET;
            addr.sin_port = SERVER_LISTEN_PORT;
            abort_if_not(inet_pton(AF_INET, server_ip_, &addr.sin_addr) > 0, "Server failed to get sockaddr");
            return addr;
        }

        // returns the index of a certain client identified using both ip and port
        // returns size_t max (-1) if the client has not registered before
        size_t indexOf(struct sockaddr_in new_client) {
            size_t rv = -1;
            pthread_mutex_lock(&lock_);
            for (size_t i = 0; i < client_count_; i++) {
                if (client_info_[i].sin_addr.s_addr == new_client.sin_addr.s_addr && client_info_[i].sin_port == new_client.sin_port) {
                    rv = i;
                    break;
                }
            }
            pthread_mutex_unlock(&lock_);
            return rv;
        }

        // returns a string representing all registered clients
        // each client registered is represented by an ip and a port
        String* get_client_infos() {
            StrBuff str_buff;

            pthread_mutex_lock(&lock_);
            for (size_t i = 0; i < client_count_; i++) {
                String *s = get_client_info(i);
                str_buff.c(*s).c(",");
                delete s;
            }

            pthread_mutex_unlock(&lock_);
            return str_buff.get();
        }

        // returns a string representing a single client
        // the client is represented by an ip and a port
        // ASSUMES THAT MUTEX HAS BEEN LOCKED
        String* get_client_info(int idx) {
            StrBuff str_buff;

            char ipstr[INET6_ADDRSTRLEN];
            inet_ntop(AF_INET, &client_info_[idx].sin_addr, ipstr, sizeof(ipstr));
            int port = client_info_[idx].sin_port;
            str_buff.c(ipstr).c(":").c(port);
            return str_buff.get();
        }

        void accept_connections() {
            printf("Server is now accepting client connections...\n");
            Network::accept_connections(server_fd_);
        }

        // send a new directory to each client registered with this server
        void broadcast_directory() {
            pthread_mutex_lock(&lock_);
            Directory dir(get_sockaddr(), client_count_, client_info_);
            int fd;

            for (size_t i = 0; i < client_count_; i++) {
                fd = connect_to(&client_info_[i]);
                send_message(fd, dir);
                close(fd);
            }

            pthread_mutex_unlock(&lock_);
        }

        // send a shudown message to each client registered with this server
        void shutdown_clients() {
            Shutdown shutdown(get_sockaddr());
            int fd;
            
            pthread_mutex_lock(&lock_);
            for (size_t i = 0; i < client_count_; i++) {
                fd = connect_to(&client_info_[i]);
                send_message(fd, shutdown);
                close(fd);
            }
            pthread_mutex_unlock(&lock_);
        }

        // to be inherited and overwritten
        // handles a connection from the given file descriptor and the sockaddr_storage with information about the connector
        virtual void handle_message(Header* message) {
            Register* r;
            Deregister* d;
            size_t idx;

            switch (message->get_type()) {
                case MsgKind::REGISTER:
                    r = dynamic_cast<Register*>(message);
                    abort_if_not(r != nullptr, "Server failed to cast Register type message.");

                    if (indexOf(r->get_sender()) == -1) {
                        pthread_mutex_lock(&lock_);
                        // enforce limit of clients that have registered
                        abort_if_not(client_count_ < CLIENT_NUM, "Server.handle_message: To many clients registered");
                        
                        client_info_[client_count_++] = r->get_sender();

                        String* client = get_client_info(client_count_-1);
                        pthread_mutex_unlock(&lock_);
                        printf("Registering new client ip: %s\n", client->c_str());
                        delete client;

                        // just print the current ips for debugging
                        String *client_ipps = get_client_infos();
                        printf("Current client ips: %s\n", client_ipps->c_str());
                        delete client_ipps;

                        // boradcast to all clients
                        broadcast_directory();
                    }
                    break;
                case MsgKind::DEREGISTER:
                    d = dynamic_cast<Deregister*>(message);
                    abort_if_not(d != nullptr, "Server failed to cast Deregister type message.");
                    idx = indexOf(d->get_sender());
                    if (idx != -1) {
                        pthread_mutex_lock(&lock_);
                        String* client = get_client_info(idx);
                        printf("Deregistering client ip: %s\n", client->c_str());
                        delete client;

                        // decrease the client count
                        client_count_--; 

                        // shift the clients 
                        for ( ; idx < client_count_; idx++) {
                            client_info_[idx] = client_info_[idx + 1];
                        }
                        pthread_mutex_unlock(&lock_);

                        // just print the current ips for debugging
                        String *client_ipps = get_client_infos();
                        printf("Current client ips: %s\n", client_ipps->c_str());
                        delete client_ipps;

                        // boradcast to all clients
                        broadcast_directory();
                    }
                    break;
                default:
                    printf("Server: Got a bad message\n");
            }
        }
};

// Client is a subclass of Network
// Client can register itself on the Server
// Client can also send and receive messages directly from other clients
class Client : public Network {
    public:
        const char *server_ip_;  // does not own
        const char* client_ip_; // does not own
        int listening_port_;
        int client_listen_fd_;
        ListeningThread* listening_thread_; // owned
        MessageHandler* msg_handler_; // not owned

        Client(const char* server_ip, const char* client_ip, int client_listen_port, MessageHandler* msg_handler) {
            server_ip_ = server_ip;
            client_ip_ = client_ip;
            listening_port_ = client_listen_port;
            msg_handler_ = msg_handler;

            // spawns a child process that listens on the given listening port
            client_listen_fd_ = get_listen_socket(client_ip_, listening_port_);

            quitting_ = false;
            listening_thread_ = new ListeningThread(this);
            listening_thread_->start();

            server_register();
        }

        ~Client() {
            // deregister before closing anything
            Deregister deregister(get_sockaddr());
            int server_fd = connect_to(server_ip_, SERVER_LISTEN_PORT);
            send_message(server_fd, deregister);

            // set the quitting flag and then wait for the listening thread to join before continuing 
            quitting_ = true;
            listening_thread_->join();
            
            delete listening_thread_;
            close(client_listen_fd_);
        }

        // get the index of this client in the directory of the server
        size_t get_index() {
            // TODO: get actual node index
            return 0;
        }

        virtual sockaddr_in get_sockaddr() {
            struct sockaddr_in addr;
            addr.sin_family = AF_INET;
            addr.sin_port = listening_port_;
            abort_if_not(inet_pton(AF_INET, client_ip_, &addr.sin_addr) > 0, "Server failed to get sockaddr");
            return addr;
        }

        // send a message to a client
        void send_string(struct sockaddr_in *destination, const char *msg) {
            int dest_sock = connect_to(destination);
            Message message(get_sockaddr(), strlen(msg) + 1, msg);
            send_message(dest_sock, message);
            close(dest_sock);
        }

        // send a message to a client
        void send_string(const char *ip, unsigned short port, const char *msg) {
            struct sockaddr_in dest;
            dest.sin_port = port;
            inet_pton(AF_INET, ip, &dest.sin_addr);
            send_string(&dest, msg);
        }

        // register this client against the server
        void server_register() {
            printf("Registering client\n");
            int dest_sock = connect_to(server_ip_, SERVER_LISTEN_PORT);
            Register message(get_sockaddr());
            send_message(dest_sock, message);
            close(dest_sock);
        }
        
        void accept_connections() {
            printf("Client is now accepting other client connections on port %d...\n", listening_port_);
            Network::accept_connections(client_listen_fd_);
        }

        // to be inherited and overwritten
        // handles a connection from the given file descriptor and the sockaddr_storage with information about the connector
        virtual void handle_message(Header* message) {
            String* dirs;

            switch (message->get_type()) {
                case MsgKind::DIRECTORY:
                {
                    Directory* m = dynamic_cast<Directory*>(message);
                    abort_if_not(m != nullptr, "Client failed to cast Directory type.");
                    
                    dirs = m->get_client_infos();

                    msg_handler_->handle_new_directory(*dirs);

                    delete dirs;
                    break;
                }
                case MsgKind::MESSAGE:
                {   
                    Message* m = dynamic_cast<Message*>(message);
                    abort_if_not(m != nullptr, "Client failed to cast Message type.");

                    char ipstr[INET6_ADDRSTRLEN];
                    sockaddr_in sender = m->get_sender();
                    inet_ntop(AF_INET, &sender.sin_addr, ipstr, sizeof(ipstr));

                    String sender_str(ipstr);
                    msg_handler_->handle_message_data(sender_str, m->get_payload());

                    break;
                }
                case MsgKind::SHUTDOWN:
                {
                    printf("Shutting down\n");
                    // TODO: clean up main thread instead of exiting
                    exit(0);
                    break;
                }
                default:
                {
                    printf("Client: Got a bad message\n");
                }
            }
        }
};