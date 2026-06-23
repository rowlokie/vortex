#include "Consumer.h"
#include <iostream>
#include <cstring>
#include <sstream>

Consumer::Consumer(const std::string& host, int port)
    : host(host), port(port), clientSocket(INVALID_SOCKET_VAL), connected(false) {}

Consumer::~Consumer() {
    disconnect();
}

bool Consumer::connectToServer() {
    if (connected) return true;

#ifdef _WIN32
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "[Consumer SDK] WSAStartup failed.\n";
        return false;
    }
#endif

    struct addrinfo hints, *res = nullptr;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    std::string portStr = std::to_string(port);
    if (getaddrinfo(host.c_str(), portStr.c_str(), &hints, &res) != 0) {
        std::cerr << "[Consumer SDK] Host resolution failed: " << host << "\n";
#ifdef _WIN32
        WSACleanup();
#endif
        return false;
    }

    clientSocket = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (clientSocket == INVALID_SOCKET_VAL) {
        std::cerr << "[Consumer SDK] Failed to create socket.\n";
        freeaddrinfo(res);
#ifdef _WIN32
        WSACleanup();
#endif
        return false;
    }

    if (connect(clientSocket, res->ai_addr, (int)res->ai_addrlen) == SOCKET_ERROR_VAL) {
        CLOSE_SOCKET(clientSocket);
        freeaddrinfo(res);
#ifdef _WIN32
        WSACleanup();
#endif
        return false;
    }

    freeaddrinfo(res);
    connected = true;
    std::cout << "[Consumer SDK] Connected to broker at " << host << ":" << port << "\n";
    return true;
}

void Consumer::disconnect() {
    if (connected) {
        if (clientSocket != INVALID_SOCKET_VAL) {
            CLOSE_SOCKET(clientSocket);
            clientSocket = INVALID_SOCKET_VAL;
        }
        connected = false;
        std::cout << "[Consumer SDK] Disconnected from broker.\n";
#ifdef _WIN32
        WSACleanup();
#endif
    }
}

long Consumer::getCommittedOffset(const std::string& topic, const std::string& consumerId) {
    if (!connected) {
        if (!connectToServer()) return 0;
    }

    std::string payload = "GET_OFFSET " + topic + " " + consumerId + "\n";
    int sentBytes = ::send(clientSocket, payload.c_str(), (int)payload.length(), 0);
    if (sentBytes <= 0) {
        std::cerr << "[Consumer SDK] Write failed on GET_OFFSET. Reconnecting...\n";
        disconnect();
        return 0;
    }

    char buffer[1024];
    std::memset(buffer, 0, sizeof(buffer));
    int receivedBytes = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
    if (receivedBytes <= 0) {
        std::cerr << "[Consumer SDK] Read failed on GET_OFFSET. Reconnecting...\n";
        disconnect();
        return 0;
    }

    std::string response(buffer);
    std::string prefix = "\"offset\":";
    size_t pos = response.find(prefix);
    if (pos != std::string::npos) {
        size_t start = pos + prefix.length();
        while (start < response.length() && (response[start] == ' ' || response[start] == ':')) {
            start++;
        }
        size_t end = start;
        while (end < response.length() && std::isdigit(response[end])) {
            end++;
        }
        try {
            return std::stol(response.substr(start, end - start));
        } catch (...) {
            return 0;
        }
    }
    return 0;
}

bool Consumer::commitOffset(const std::string& topic, const std::string& consumerId, long offset) {
    if (!connected) {
        if (!connectToServer()) return false;
    }

    std::string payload = "COMMIT " + topic + " " + consumerId + " " + std::to_string(offset) + "\n";
    int sentBytes = ::send(clientSocket, payload.c_str(), (int)payload.length(), 0);
    if (sentBytes <= 0) {
        std::cerr << "[Consumer SDK] Write failed on COMMIT. Reconnecting...\n";
        disconnect();
        return false;
    }

    char buffer[1024];
    std::memset(buffer, 0, sizeof(buffer));
    int receivedBytes = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
    if (receivedBytes <= 0) {
        std::cerr << "[Consumer SDK] Read failed on COMMIT. Reconnecting...\n";
        disconnect();
        return false;
    }

    std::string response(buffer);
    return (response.find("\"status\":\"OK\"") != std::string::npos);
}

std::vector<Record> Consumer::poll(const std::string& topic, int lastOffset) {
    std::vector<Record> records;
    
    if (!connected) {
        if (!connectToServer()) {
            return records;
        }
    }

    std::string payload = "CONSUME " + topic + " " + std::to_string(lastOffset) + "\n";
    int sentBytes = ::send(clientSocket, payload.c_str(), (int)payload.length(), 0);
    if (sentBytes <= 0) {
        std::cerr << "[Consumer SDK] Write failed on CONSUME. Reconnecting...\n";
        disconnect();
        return records;
    }

    std::string response;
    char buffer[1024];
    while (true) {
        std::memset(buffer, 0, sizeof(buffer));
        int receivedBytes = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
        if (receivedBytes <= 0) {
            std::cerr << "[Consumer SDK] Read failed on CONSUME. Reconnecting...\n";
            disconnect();
            break;
        }
        buffer[receivedBytes] = '\0';
        response += buffer;

        // Check if the response ends with "\n\n" or if it is just "\n" (empty result)
        if (response == "\n" || (response.length() >= 2 && response.substr(response.length() - 2) == "\n\n")) {
            break;
        }
    }

    std::stringstream ss(response);
    std::string line;
    while (std::getline(ss, line)) {
        if (line.empty()) {
            break; // Stop on terminating blank line
        }

        // Parse offset|timestamp|message
        size_t firstPipe = line.find('|');
        size_t secondPipe = line.find('|', firstPipe + 1);
        if (firstPipe != std::string::npos && secondPipe != std::string::npos) {
            try {
                long offset = std::stol(line.substr(0, firstPipe));
                long ts = std::stol(line.substr(firstPipe + 1, secondPipe - firstPipe - 1));
                std::string msg = line.substr(secondPipe + 1);
                records.push_back({offset, ts, msg});
            } catch (...) {
                // Ignore malformed lines
            }
        }
    }

    return records;
}
