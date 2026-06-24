// broker/GroupManager.h
#pragma once

#include "ConsumerGroup.h"
#include <unordered_map>
#include <string>
#include <vector>

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

class GroupManager {
private:
    std::unordered_map<std::string, ConsumerGroup> groups;
    std::unordered_map<std::string, std::string> consumerToGroup;
    PlatformMutex groupsMutex;
    
    void rebalanceGroup(const std::string& groupId);
    
public:
    GroupManager();
    ~GroupManager();
    
    bool joinGroup(const std::string& groupId, 
                   const std::string& topic, 
                   const std::string& consumerId);
    
    bool leaveGroup(const std::string& groupId, const std::string& consumerId);
    
    bool heartbeat(const std::string& groupId, const std::string& consumerId);
    
    std::vector<int> getConsumerPartitions(const std::string& groupId, 
                                           const std::string& consumerId);
    
    bool assignPartitions(const std::string& groupId, int partitionCount);
    
    std::string getConsumerForPartition(const std::string& groupId, int partition);
    
    std::vector<std::string> getGroupConsumers(const std::string& groupId);
    
    bool isConsumerInGroup(const std::string& groupId, const std::string& consumerId);
    
    void cleanupDeadConsumers();
    
    bool groupExists(const std::string& groupId) const;
    
    std::string getGroupTopic(const std::string& groupId) const;
};