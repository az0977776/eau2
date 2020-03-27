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
#include <errno.h>

#include "../util/helper.h"
#include "../util/object.h"
#include "../util/string.h"
#include "../util/serial.h"
#include "../util/thread.h"

static const int SERVER_LISTEN_PORT = 8080;
static const int CLIENT_NUM = 5;
static const int BUFF_LEN = 1024;
static const size_t MAX_SIZE_T = -1;

// to be implemented from
// this is a class that has a callback method to be called when a message is returned
class MessageHandler : public Object {
    public:
    
        // handle a genaric message coming from the given sender
        // return: the response the the given message.
        //          if respnse is nullptr then nothing is sent back.
        virtual Response* handle_message(sockaddr_in server, size_t data_len, char* data) { 
            return nullptr;
        }

        // handle a genaric message coming from the given sender
        // return: the response the the given message.
        //          if respnse is nullptr then nothing is sent back.
        virtual Response* handle_get(sockaddr_in server, size_t data_len, char* data) {
            return nullptr;
        }

        // handle a genaric message coming from the given sender
        // return: the response the the given message.
        //          if respnse is nullptr then nothing is sent back.
        virtual Response* handle_get_and_wait(sockaddr_in server, size_t data_len, char* data) {
            return nullptr;
        }
        
        // handle a genaric message coming from the given sender
        // return: the response the the given message.
        //          if respnse is nullptr then nothing is sent back.
        virtual Response* handle_put(sockaddr_in server, size_t data_len, char* data) {
            return nullptr;
        }
};

class Network;

class ConnectionThread : public Thread {
    public:
        Network* network_;  // does not own
        int fd_;

        ConnectionThread(Network* network) {
            network_ = network;
            fd_ = 0;
        }

        ~ConnectionThread() { }

        void set_fd(int fd);
        void run();    
};

// Network is a subclass of Object
// Network abstracts creating a listening socket, accepting connections, 
// connecting to a remote socket and sending messages to sockets.
class Network : public Object {
    public:
        bool quitting_; // is this network object quitting
        ConnectionThread** connection_threads_;
        size_t connection_count_;

        Network() {
            quitting_ = false;
            connection_threads_ = new ConnectionThread*[CLIENT_NUM];
            for (size_t i = 0; i < CLIENT_NUM; i++) {
                connection_threads_[i] = new ConnectionThread(this);
            }
            connection_count_ = 0;
        }

        ~Network() {
            for (size_t i = 0; i < CLIENT_NUM; i++) {
                delete connection_threads_[i];
            }
            delete[] connection_threads_;
        }

        // sends a char* to the given file descriptor
        // returns true if all characters were sent, false otherwise
        bool send_chars(int fd, size_t num_bytes, const char *c) {
            return (size_t)(send(fd, c, num_bytes, 0)) == num_bytes;
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

        void send_ready_(int fd) {
            Ready ready(get_sockaddr());
            // send the ready 
            abort_if_not(send_chars(fd, ready.header_len(), ready.get_header()), "Failed to send ready header");
        }

        void send_ack_(int fd) {
            Ack ack(get_sockaddr());
            // Send an ack to close the connection
            abort_if_not(send_chars(fd, ack.header_len(), ack.get_header()), "Failed to send ack header");
        }

        // Sends the given header over the given file descriptor
        char* send_message(int fd, Header &header, size_t &return_msg_len) {
            char* buf = new char[HEADER_SIZE];
            Header* check_header2;
            char* rv = nullptr;
            return_msg_len = 0;

            // sending the header to the file descriptor
            abort_if_not(send_chars(fd, header.header_len(), header.get_header()), "Failed to send header");

            int read_bytes = read(fd, buf, header.header_len());
            printf("Number of bytes returned for read: %d, header_len = %zu\n", read_bytes, header.header_len());
            abort_if_not((size_t)read_bytes == header.header_len(), "send_message().check_header: Did not read the correct numnber of bytes");
            Header check_header(buf);

            // check that the returned header type
            switch (check_header.get_type()) {
                case MsgKind::ACK:
                    // ACK means protocall is finished
                    break;
                case MsgKind::READY:
                    // the reciever is ready for the data so send the payload (i.e. get key)
                    send_payload_(fd, header);
                    
                    // check what is returned
                    abort_if_not((size_t)(read(fd, buf, header.header_len())) == header.header_len(), "send_message().check_header2: Did not read the correct numnber of bytes");
                    check_header2 = new Header(buf);
                    
                    // returned header should be an ACK to close the connection
                    if (check_header2->get_type() == MsgKind::RESPONSE) {
                        send_ready_(fd);

                        // get response
                        return_msg_len = check_header2->get_payload_size();
                        rv = receive_payload_(fd, check_header2->get_payload_size());
                        
                        send_ack_(fd);
                    } else if (check_header2->get_type() != MsgKind::ACK) {
                        abort_if_not(false, "Send message did not get an ack to close connection");
                    }

                    delete check_header2;
                    break;
                default:
                    // any other type at this time is an error
                    abort_if_not(false, "Did not get a 'ready' or 'ack' message back after sending header");
                    break;
            }
            delete[] buf;

            return rv;
        }

        // Recieve the a payload of the given size over the given file descriptor. This will recieve the payload in 
        // packets of size BUFF_LEN or less. Returns a buffer of the given size.
        char* receive_payload_(int fd, size_t payload_size) {
            char* payload = new char[payload_size];

            for (size_t i = 0; i < payload_size; i += BUFF_LEN) {
                size_t packet_size = payload_size - i < BUFF_LEN? payload_size - i : BUFF_LEN;
                // printf("packet_size to read: %zu\n", packet_size);
                abort_if_not(read(fd, payload + i, packet_size), "failed to receive a packet payload");
            }
            return payload;
        }

        // This is a method to be overridden. Gets the sockaddr_in that represents this network object
        virtual sockaddr_in get_sockaddr() {
            return { 0 };
        }

        template<class T> Header* read_payload_(int fd, Header& check_header) {
            // send the ready to recieve message and start reading from the file descriptor
            send_ready_(fd);
            char* payload = receive_payload_(fd, check_header.get_payload_size());

            // deserialize the payload into a message. Set up return value.
            Header* rv = new T(check_header.get_sender(), check_header.get_payload_size(), payload);
            delete payload;

            return rv;
        }

        Header* recieve_message(int fd) {
            Header* rv = nullptr;
            char* buf = new char[HEADER_SIZE];

            // read in the header of a message
            abort_if_not(read(fd, buf, HEADER_SIZE), "Failed to receive message header");
            Header check_header(buf);

            // check what the message is
            switch(check_header.get_type()) {
                case MsgKind::REGISTER:
                    rv = read_payload_<Register>(fd, check_header);
                    break;
                case MsgKind::DEREGISTER:
                    rv = read_payload_<Deregister>(fd, check_header);
                    break;
                case MsgKind::DIRECTORY:
                    rv = read_payload_<Directory>(fd, check_header);
                    break;
                case MsgKind::GET:
                    rv = read_payload_<Get>(fd, check_header);
                    break;
                case MsgKind::GETANDWAIT:
                    rv = read_payload_<GetAndWait>(fd, check_header);
                    break;
                case MsgKind::PUT:
                    rv = read_payload_<Put>(fd, check_header);
                    break;
                case MsgKind::MESSAGE:
                    rv = read_payload_<Message>(fd, check_header);
                    break;
                case MsgKind::RESPONSE:
                    rv = read_payload_<Response>(fd, check_header);
                    break;
                case MsgKind::SHUTDOWN:
                    // Send an ack to close the connection
                    send_ack_(fd);
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
        virtual Response* handle_message(Header* message) {
            return nullptr;
        }

        // to be inherited and overwritten
        // this accepts connections on the default listening file descriptor
        virtual void accept_connections() { }
        
        // continues to accept and handle connections on the given file descriptor
        void accept_connections(int fd) {
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
                    
                    // can receive up to CLIENT_NUM connections at once
                    ConnectionThread *c = connection_threads_[connection_count_++ % CLIENT_NUM];
                    c->set_fd(new_fd);
                    c->start();
                    c->detach();
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

void ConnectionThread::set_fd(int fd) {
    fd_ = fd;
}

void ConnectionThread::run() {
    printf("connection thread starting\n");
    Header* message = network_->recieve_message(fd_);
    printf("connection thread received message\n");
    Response* response = network_->handle_message(message);
    printf("created a response for the received message\n");
    size_t return_msg_len;

    if (response != nullptr) {
        // user wants to send a response to the fd;
        network_->send_message(fd_, *response, return_msg_len);
        abort_if_not(return_msg_len == 0, "Sent a response and got a buf back");
        delete response;
    }

    network_->send_ack_(fd_);
    printf("sending an ack\n");

    delete message;
    message = nullptr;
    close(fd_);
}

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

        Server(const char* server_ip) : Network() {
            abort_if_not(pthread_mutex_init(&lock_, NULL) == 0, "Client: Failed to create mutex");
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
            sockaddr_in s = get_sockaddr();
            printf("Server is now accepting client connections on port: %d...\n", s.sin_port);
            Network::accept_connections(server_fd_);
        }

        // send a new directory to each client registered with this server
        void broadcast_directory() {
            pthread_mutex_lock(&lock_);
            Directory dir(get_sockaddr(), client_count_, client_info_);

            int fd;
            size_t resp_size;
            for (size_t i = 0; i < client_count_; i++) {
                fd = connect_to(&client_info_[i]);
                send_message(fd, dir, resp_size);
                abort_if_not(resp_size == 0, "Server broadcast directory: Got a response back");

                close(fd);
            }

            pthread_mutex_unlock(&lock_);
        }

        // send a shudown message to each client registered with this server
        void shutdown_clients() {
            Shutdown shutdown(get_sockaddr());
            int fd;
            size_t resp_size;
            
            pthread_mutex_lock(&lock_);
            for (size_t i = 0; i < client_count_; i++) {
                fd = connect_to(&client_info_[i]);
                send_message(fd, shutdown, resp_size);
                abort_if_not(resp_size == 0, "Server broadcast shutdown: Got a response back");
                close(fd);
            }
            pthread_mutex_unlock(&lock_);
        }

        // to be inherited and overwritten
        // handles a connection from the given file descriptor and the sockaddr_storage with information about the connector
        virtual Response* handle_message(Header* message) {
            Register* r;
            Deregister* d;
            size_t idx;

            switch (message->get_type()) {
                case MsgKind::REGISTER:
                    r = dynamic_cast<Register*>(message);
                    abort_if_not(r != nullptr, "Server failed to cast Register type message.");

                    if (indexOf(r->get_sender()) == MAX_SIZE_T) {
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
                    if (idx != MAX_SIZE_T) {
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
            return nullptr;
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

        pthread_mutex_t lock_;  // this is a lock for the directory
        Directory* current_dir_;  // owned
        bool directory_init_;

        size_t current_node_idx_;

        Client(const char* server_ip, const char* client_ip, MessageHandler* msg_handler) : Network() {
            server_ip_ = server_ip;
            client_ip_ = client_ip;
            msg_handler_ = msg_handler;

            abort_if_not(pthread_mutex_init(&lock_, NULL) == 0, "Client: Failed to create mutex");
            current_dir_ = nullptr;
            directory_init_ = false;

            // spawns a child process that listens on the given listening port
            client_listen_fd_ = get_listen_socket(client_ip_, 0);

            sockaddr_in client_listen_addr;
            socklen_t len = sizeof(struct sockaddr);
            getsockname(client_listen_fd_, (struct sockaddr *)&client_listen_addr, &len);
            
            listening_port_ = ntohs(client_listen_addr.sin_port);

            quitting_ = false;
            listening_thread_ = new ListeningThread(this);
            listening_thread_->start();

            server_register();

            // blocks until current_dir_ is set
            // when this returns, this thread has the lock
            wait_for_dir_();
      
            current_node_idx_ = current_dir_->index_of(client_ip, listening_port_);
            abort_if_not(current_node_idx_ != MAX_SIZE_T, "Failed to find client in directory");

            printf("current node idx: %zu\n", current_node_idx_);

            pthread_mutex_unlock(&lock_);
        }

        ~Client() {
            size_t resp_size;
            // deregister before closing anything
            Deregister deregister(get_sockaddr());
            int server_fd = connect_to(server_ip_, SERVER_LISTEN_PORT);
            send_message(server_fd, deregister, resp_size);
            abort_if_not(resp_size == 0, "Client deregister: Got a response back");

            // set the quitting flag and then wait for the listening thread to join before continuing 
            quitting_ = true;
            listening_thread_->join();


            pthread_mutex_lock(&lock_);
            if (current_dir_ != nullptr) {
                delete current_dir_;
            }
            pthread_mutex_unlock(&lock_);
            
            delete listening_thread_;
            close(client_listen_fd_);

            pthread_mutex_destroy(&lock_); 
        }

        MessageHandler* get_message_handler() {
            return msg_handler_;
        }

        // NOTE: grabs the lock for the directory
        void wait_for_dir_() {
            while(true) {
                pthread_mutex_lock(&lock_);
                if (directory_init_) {
                    break; // don't unlock until current_node_idx_ is found
                }
                pthread_mutex_unlock(&lock_); // unlock so dir can be set
                sleep(1);  // some time to wait
            }
        }

        // get the index of this client in the directory of the server
        size_t get_index() {
            return current_node_idx_;
        }

        size_t num_nodes() {
            pthread_mutex_lock(&lock_);
            size_t ret_val = current_dir_->get_num_clients();
            pthread_mutex_unlock(&lock_);
            return ret_val;
        }

        virtual sockaddr_in get_sockaddr() {
            struct sockaddr_in addr;
            addr.sin_family = AF_INET;
            addr.sin_port = listening_port_;
            abort_if_not(inet_pton(AF_INET, client_ip_, &addr.sin_addr) > 0, "Server failed to get sockaddr");
            return addr;
        }

        char* send_to_node_(size_t to_node_idx, Header* message, size_t &return_msg_len) {
            pthread_mutex_lock(&lock_);
            int dest_sock = connect_to(current_dir_->get(to_node_idx));
            pthread_mutex_unlock(&lock_);

            char* rv = send_message(dest_sock, *message, return_msg_len);
            close(dest_sock);
            return rv;
        }

        // Getting a value associated with a key on another key value store client
        char* get(size_t to_node_idx, size_t payload_len, char* payload, size_t &return_msg_len) {
            Get message(get_sockaddr(), payload_len, payload);
            
            return send_to_node_(to_node_idx, &message, return_msg_len);
        }
        
        // Getting a value associated with a key on another key value store client
        char* get_and_wait(size_t to_node_idx, size_t payload_len, char* payload, size_t &return_msg_len) {
            GetAndWait message(get_sockaddr(), payload_len, payload);
            
            return send_to_node_(to_node_idx, &message, return_msg_len);
        }

        void put(size_t to_node_idx, size_t payload_len, char* payload) {
            Put message(get_sockaddr(), payload_len, payload);
            size_t return_msg_len = 0;
            
            char* rv = send_to_node_(to_node_idx, &message, return_msg_len);
            abort_if_not(return_msg_len == 0 && rv == nullptr, "Got a response from a put message");
        }

        // register this client against the server
        void server_register() {
            size_t resp_size;
            int dest_sock = connect_to(server_ip_, SERVER_LISTEN_PORT);
            Register message(get_sockaddr());
            send_message(dest_sock, message, resp_size);
            abort_if_not(resp_size == 0, "Server Register: Got a response back");
            close(dest_sock);
        }
        
        void accept_connections() {
            Network::accept_connections(client_listen_fd_);
        }

        Response* message_handler_dispatch(Header* message) {
            Message* msg = dynamic_cast<Message*>(message);
            abort_if_not(msg != nullptr, "Client failed to cast Message type.");

            switch (message->get_type()) {
                case MsgKind::GET:
                {   
                    return msg_handler_->handle_get(get_sockaddr(), msg->get_payload_size(), msg->get_payload());
                }
                case MsgKind::GETANDWAIT:
                {
                    return msg_handler_->handle_get_and_wait(get_sockaddr(), msg->get_payload_size(), msg->get_payload());
                }
                case MsgKind::PUT:
                {
                    return msg_handler_->handle_put(get_sockaddr(), msg->get_payload_size(), msg->get_payload());
                }
                case MsgKind::MESSAGE:
                {  
                    return msg_handler_->handle_message(get_sockaddr(), msg->get_payload_size(), msg->get_payload());
                }
                default:
                    fail("message_handler_dispatch: got an invalid msgkind");
                    return nullptr;
            }
        }

        // to be inherited and overwritten
        // handles a connection from the given file descriptor and the sockaddr_storage with information about the connector
        virtual Response* handle_message(Header* message) {
            Response* rv = nullptr;
            switch (message->get_type()) {
                case MsgKind::DIRECTORY:
                {
                    Directory* new_dir = dynamic_cast<Directory*>(message);
                    abort_if_not(new_dir != nullptr, "Client failed to cast Directory type.");

                    pthread_mutex_lock(&lock_);
                    if (current_dir_ != nullptr) {
                        delete current_dir_;
                    }
                    current_dir_ = new_dir->clone();

                    directory_init_ = true;
                    pthread_mutex_unlock(&lock_);
                    break;
                }
                case MsgKind::GET:
                case MsgKind::GETANDWAIT:
                case MsgKind::PUT:
                case MsgKind::MESSAGE:
                {  
                    rv = message_handler_dispatch(message);
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

            return rv;
        }

        // send a message to a client
        void send_string(struct sockaddr_in *destination, const char *msg) {
            size_t resp_size;
            int dest_sock = connect_to(destination);
            Message message(get_sockaddr(), strlen(msg) + 1, msg);
            send_message(dest_sock, message, resp_size);
            abort_if_not(resp_size == 0, "send_string: bad resp_size");
            close(dest_sock);
        }

        // send a message to a client
        void send_string(const char *ip, unsigned short port, const char *msg) {
            struct sockaddr_in dest;
            dest.sin_port = port;
            inet_pton(AF_INET, ip, &dest.sin_addr);
            send_string(&dest, msg);
        }
};