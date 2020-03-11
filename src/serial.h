//lang:CwC
#pragma once
#include "string.h"
#include <stdio.h>
#include <netinet/in.h>

enum class MsgKind { 
    HEADER,
    REGISTER,
    DIRECTORY,
    MESSAGE,
    SHUTDOWN,
    READY,
    ACK,
    DEREGISTER
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

        Ack(sockaddr_in sender, char* bytes) : Header(MsgKind::ACK, 0, sender) { }

        ~Ack() { }
};

class Ready : public Header {
    public:
        Ready(sockaddr_in sender) : Header(MsgKind::READY, 0, sender) { }

        Ready(sockaddr_in sender, char* bytes) : Header(MsgKind::READY, 0, sender) { }

        ~Ready() { }
};

class Shutdown : public Header {
    public:
        Shutdown(sockaddr_in sender) : Header(MsgKind::SHUTDOWN, 0, sender) { }

        Shutdown(sockaddr_in sender, char* bytes) : Header(MsgKind::SHUTDOWN, 0, sender) { }

        ~Shutdown() { }
};

class Register : public Header {
    public:
        sockaddr_in client_;

        Register(sockaddr_in client) : Header(MsgKind::REGISTER, sizeof(sockaddr_in), client) {
            client_ = client;
        }

        Register(sockaddr_in sender, char* bytes) : Header(MsgKind::REGISTER, sizeof(sockaddr_in), sender) {
            memcpy(&client_, bytes, sizeof(sockaddr_in));
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

        Deregister(sockaddr_in sender, char* bytes) : Header(MsgKind::DEREGISTER, sizeof(sockaddr_in), sender) {
            memcpy(&client_, bytes, sizeof(sockaddr_in));
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

        Directory(sockaddr_in sender, char* bytes) : Header(MsgKind::DIRECTORY, 0, sender) {
            memcpy(&num_clients_, bytes, sizeof(size_t));
            bytes += sizeof(size_t);

            payload_size_ = sizeof(size_t) + num_clients_ * sizeof(sockaddr_in);

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

};