//lang:CwC
#pragma once
#include "string.h"
#include <stdio.h>
#include <netinet/in.h>

enum class MsgKind { 
    HEADER,         // 0
    REGISTER,       // 1
    DIRECTORY,      // 2
    MESSAGE,        // 3
    SHUTDOWN,       // 4
    READY,          // 5
    ACK,            // 6
    DEREGISTER,     // 7
    GET,            // 8
    GETANDWAIT,     // 9
    PUT,            // 10
    RESPONSE,       // 11
};

const size_t HEADER_SIZE = sizeof(MsgKind) + sizeof(size_t) + sizeof(sockaddr_in);

class Header : public Object {
    public:
        MsgKind kind_;  // the message kind
        size_t payload_size_;
        sockaddr_in sender_;

        Header(MsgKind mk, size_t payload_size, sockaddr_in sender) {
            kind_ = mk;
            payload_size_ = payload_size;
            sender_ = sender;
        }

        Header(char* bytes) {
            memcpy(&kind_, bytes, sizeof(MsgKind));
            bytes+=sizeof(MsgKind);
            memcpy(&payload_size_, bytes, sizeof(size_t));
            bytes+=sizeof(size_t);
            memcpy(&sender_, bytes, sizeof(sockaddr_in));
        }

        char* get_header() {
            char* buf = new char[header_len()];
            get_header(buf);
            return buf;
        }

        void get_header(char* buf) {
            memcpy(buf, &kind_, sizeof(MsgKind));
            memcpy(buf + sizeof(MsgKind), &payload_size_, sizeof(size_t));
            memcpy(buf + sizeof(MsgKind) + sizeof(size_t), &sender_, sizeof(sockaddr_in));
        }

        virtual char* serialize() {
            return nullptr;
        }

        virtual void serialize(char* buf) {
            memset(buf, 0, header_len());
        }

        size_t header_len() {
            return HEADER_SIZE;
        }

        size_t get_payload_size() {
            return payload_size_;
        }

        MsgKind get_type() {
            return kind_;
        }

        sockaddr_in get_sender() {
            return sender_;
        }
};

class Message : public Header {
    public:
        char* payload_; // owned

        Message(sockaddr_in sender, size_t payload_size, const char* payload) : Header(MsgKind::MESSAGE, payload_size, sender) {
            payload_ = new char[payload_size];
            memcpy(payload_, payload, payload_size);
        }

        ~Message() {
            delete[] payload_;
        }

        char* serialize() {
            char* tmp = new char[get_payload_size()];
            serialize(tmp);
            return tmp;
        }

        void serialize(char* tmp) {
            memcpy(tmp, payload_, get_payload_size());
        }

        char* get_payload() {
            return payload_;
        }
};

class Ack : public Header {
    public:
        Ack(sockaddr_in sender) : Header(MsgKind::ACK, 0, sender) { }

        Ack(sockaddr_in sender, size_t payload_size, char* bytes) : Header(MsgKind::ACK, payload_size, sender) { }

        ~Ack() { }
};

class Ready : public Header {
    public:
        Ready(sockaddr_in sender) : Header(MsgKind::READY, 0, sender) { }

        Ready(sockaddr_in sender, size_t payload_size, char* bytes) : Header(MsgKind::READY, payload_size, sender) { }

        ~Ready() { }
};

class Shutdown : public Header {
    public:
        Shutdown(sockaddr_in sender) : Header(MsgKind::SHUTDOWN, 0, sender) { }

        Shutdown(sockaddr_in sender, size_t payload_size, char* bytes) : Header(MsgKind::SHUTDOWN, payload_size, sender) { }

        ~Shutdown() { }
};

class Register : public Header {
    public:
        sockaddr_in client_;

        Register(sockaddr_in client) : Header(MsgKind::REGISTER, sizeof(sockaddr_in), client) {
            client_ = client;
        }

        Register(sockaddr_in sender, size_t payload_size, char* bytes) : Header(MsgKind::REGISTER, sizeof(sockaddr_in), sender) {
            abort_if_not(payload_size == sizeof(sockaddr_in), "Register: payload size is incorrect");
            memcpy(&client_, bytes, payload_size);
        }

        ~Register() {
        }

        char* serialize() {
            char* tmp = new char[get_payload_size()];
            serialize(tmp);
            return tmp;
        }

        void serialize(char* tmp) {
            memcpy(tmp, &client_, sizeof(sockaddr_in));
        }
};

class Deregister : public Header {
    public:
        sockaddr_in client_;

        Deregister(sockaddr_in client) : Header(MsgKind::DEREGISTER, sizeof(sockaddr_in), client) {
            client_ = client;
        }

        Deregister(sockaddr_in sender, size_t payload_size, char* bytes) : Header(MsgKind::DEREGISTER, sizeof(sockaddr_in), sender) {
            abort_if_not(payload_size == sizeof(sockaddr_in), "Deregister: payload size does not match");
            memcpy(&client_, bytes, payload_size);
        }

        ~Deregister() {
        }

        char* serialize() {
            char* tmp = new char[get_payload_size()];
            serialize(tmp);
            return tmp;
        }

        void serialize(char* tmp) {
            memcpy(tmp, &client_, sizeof(sockaddr_in));
        }
};

class Directory : public Header {
    public:
        size_t num_clients_;
        sockaddr_in* clients_;  // owned

        // client is the len of both ports and addresses
        Directory(sockaddr_in sender, size_t num_clients, sockaddr_in* clients) : Header(MsgKind::DIRECTORY, sizeof(size_t) + num_clients * sizeof(sockaddr_in), sender) {
            num_clients_ = num_clients;
            clients_ = new sockaddr_in[num_clients_];
            memcpy(clients_, clients, num_clients_ * sizeof(sockaddr_in));
        }

        Directory(sockaddr_in sender, size_t payload_size, char* bytes) : Header(MsgKind::DIRECTORY, payload_size, sender) {
            memcpy(&num_clients_, bytes, sizeof(size_t));
            bytes += sizeof(size_t);

            payload_size_ = sizeof(size_t) + num_clients_ * sizeof(sockaddr_in);
            abort_if_not(payload_size_ == payload_size, "Directory: payload size does not match");

            clients_ = new sockaddr_in[num_clients_];
            memcpy(clients_, bytes, num_clients_ * sizeof(sockaddr_in));
        }

        ~Directory() {
            delete[] clients_;
        }

        char* serialize() {
            char* tmp = new char[get_payload_size()];
            serialize(tmp);
            return tmp;
        }

        void serialize(char* buf) {
            memcpy(buf, &num_clients_, sizeof(size_t));
            memcpy(buf + sizeof(size_t), clients_, num_clients_ * sizeof(sockaddr_in));
        }

        size_t get_num_clients() {
            return num_clients_;
        }

        size_t index_of(const char* client_ip, unsigned short client_listen_port) {
            // convert ip and port to sockaddr_in
            struct sockaddr_in client;
            client.sin_port = client_listen_port;
            inet_pton(AF_INET, client_ip, &client.sin_addr);

            for (size_t i = 0; i < num_clients_; i++) {
                if (client.sin_addr.s_addr == clients_[i].sin_addr.s_addr && client.sin_port == clients_[i].sin_port) {
                    return i;
                }
            }
            return -1;
        }

        sockaddr_in* get(size_t idx) {
            abort_if_not(idx < num_clients_, "Directory trying to get a node that does not exist");
            return &(clients_[idx]);
        }

        // returns a string representing all registered clients
        // each client registered is represented by an ip and a port
        String* get_client_infos() {
            StrBuff str_buff;

            for (size_t i = 0; i < num_clients_; i++) {
                String *s = get_client_info(i);
                str_buff.c(*s).c(", ");
                delete s;
            }
            return str_buff.get();
        }

        // returns a string representing a single client
        // the client is represented by an ip and a port
        String* get_client_info(int idx) {
            StrBuff str_buff;

            char ipstr[INET6_ADDRSTRLEN];
            inet_ntop(AF_INET, &clients_[idx].sin_addr, ipstr, sizeof(ipstr));
            int port = clients_[idx].sin_port;
            str_buff.c(ipstr).c(":").c(port);
            return str_buff.get();
        }

        Directory* clone() {
            return new Directory(get_sender(), num_clients_, clients_);
        }

};

class Get : public Message {
    public:
        Get(sockaddr_in sender, size_t payload_size, const char* payload) : Message(sender, payload_size, payload) {
            kind_ = MsgKind::GET;
        }

        ~Get() { }
};

class GetAndWait : public Message {
    public:
        GetAndWait(sockaddr_in sender, size_t payload_size, const char* payload) : Message(sender, payload_size, payload) {
            kind_ = MsgKind::GETANDWAIT;
        }

        ~GetAndWait() { }
};

class Put : public Message {
    public:
        Put(sockaddr_in sender, size_t payload_size, const char* payload) : Message(sender, payload_size, payload) {
            kind_ = MsgKind::PUT;
        }

        ~Put() { }
};

class Response : public Message {
    public:
        Response(sockaddr_in sender, size_t payload_size, const char* payload) : Message(sender, payload_size, payload) {
            kind_ = MsgKind::RESPONSE;
        }

        ~Response() { }
};

