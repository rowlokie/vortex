// broker/ConsumerGroup.h
#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <chrono>

#ifdef _WIN32
    #ifndef _WIN32_WINNT
        #define _WIN32_WINNT 0x0600
    #endif
    #include <windows.h>
    typedef CRITICAL_SECTION PlatformMutex;
#else
    #include <mutex>
    typedef std::mutex PlatformMutex;
#endif

struct ConsumerInfo {
    std::string consumerId;
    std::chrono::steady_clock::time_point lastHeartbeat;
    bool active;
    
    // Default constructor - FIX: Added this
    ConsumerInfo() : consumerId(""), 
                     lastHeartbeat(std::chrono::steady_clock::now()),
                     active(true) {}
    
    // Parameterized constructor
    ConsumerInfo(const std::string& id) 
        : consumerId(id), 
          lastHeartbeat(std::chrono::steady_clock::now()),
          active(true) {}
};

class ConsumerGroup {
public:
    std::string groupId;
    std::string topic;
    std::unordered_map<std::string, ConsumerInfo> consumers;
    std::unordered_map<int, std::string> partitionAssignments;
    PlatformMutex groupMutex;
    
    ConsumerGroup() : groupId(""), topic("") {
#ifdef _WIN32
        InitializeCriticalSection(&groupMutex);
#endif
    }
    
    ConsumerGroup(const std::string& id, const std::string& topicName) 
        : groupId(id), topic(topicName) {
#ifdef _WIN32
        InitializeCriticalSection(&groupMutex);
#endif
    }
    
    ~ConsumerGroup() {
#ifdef _WIN32
        DeleteCriticalSection(&groupMutex);
#endif
    }
    
    // Not copyable
    ConsumerGroup(const ConsumerGroup&) = delete;
    ConsumerGroup& operator=(const ConsumerGroup&) = delete;
    
    void addConsumer(const std::string& consumerId);
    void removeConsumer(const std::string& consumerId);
    void updateHeartbeat(const std::string& consumerId);
    int getConsumerCount() const;
    std::vector<std::string> getActiveConsumers();
    void clearAssignments();
};