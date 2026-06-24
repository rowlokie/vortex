// broker/GroupManager.cpp
#include "GroupManager.h"
#include <iostream>
#include <algorithm>
#include <chrono>

class GroupLockGuard {
    PlatformMutex* mtx;
public:
    GroupLockGuard(PlatformMutex* m) : mtx(m) {
#ifdef _WIN32
        EnterCriticalSection(mtx);
#else
        mtx->lock();
#endif
    }
    ~GroupLockGuard() {
#ifdef _WIN32
        LeaveCriticalSection(mtx);
#else
        mtx->unlock();
#endif
    }
};

GroupManager::GroupManager() {
#ifdef _WIN32
    InitializeCriticalSection(&groupsMutex);
#endif
}

GroupManager::~GroupManager() {
#ifdef _WIN32
    DeleteCriticalSection(&groupsMutex);
#endif
}

void GroupManager::rebalanceGroup(const std::string& groupId) {
    GroupLockGuard lock(&groupsMutex);
    
    auto groupIt = groups.find(groupId);
    if (groupIt == groups.end()) {
        return;
    }
    
    auto& group = groupIt->second;
    
    // Get active consumers
    std::vector<std::string> activeConsumers = group.getActiveConsumers();
    
    if (activeConsumers.empty()) {
        group.clearAssignments();
        return;
    }
    
    // Get all partitions that are currently assigned
    std::vector<int> allPartitions;
    for (auto& pair : group.partitionAssignments) {
        allPartitions.push_back(pair.first);
    }
    
    // Sort partitions
    std::sort(allPartitions.begin(), allPartitions.end());
    
    // Reassign partitions round-robin
    std::unordered_map<int, std::string> newAssignments;
    
    for (size_t i = 0; i < allPartitions.size(); i++) {
        int partition = allPartitions[i];
        std::string consumer = activeConsumers[i % activeConsumers.size()];
        newAssignments[partition] = consumer;
    }
    
    group.partitionAssignments = newAssignments;
    
    std::cout << "[REBALANCE] Group " << groupId << " rebalanced. "
              << activeConsumers.size() << " consumers, "
              << allPartitions.size() << " partitions" << std::endl;
}

bool GroupManager::joinGroup(const std::string& groupId, 
                             const std::string& topic, 
                             const std::string& consumerId) {
    GroupLockGuard lock(&groupsMutex);
    
    // Check if consumer is already in another group
    auto consumerIt = consumerToGroup.find(consumerId);
    if (consumerIt != consumerToGroup.end() && consumerIt->second != groupId) {
        // Remove from old group first
        leaveGroup(consumerIt->second, consumerId);
    }
    
    // Find or create group
    auto groupIt = groups.find(groupId);
    if (groupIt == groups.end()) {
        // Use emplace to construct ConsumerGroup in place
        groups.emplace(std::piecewise_construct,
                       std::forward_as_tuple(groupId),
                       std::forward_as_tuple(groupId, topic));
        groupIt = groups.find(groupId);
    }
    
    // Add consumer to group
    groupIt->second.addConsumer(consumerId);
    consumerToGroup[consumerId] = groupId;
    
    // Trigger rebalance
    rebalanceGroup(groupId);
    
    std::cout << "[JOIN_GROUP] Consumer " << consumerId 
              << " joined group " << groupId 
              << " for topic " << topic << std::endl;
    
    return true;
}

bool GroupManager::leaveGroup(const std::string& groupId, const std::string& consumerId) {
    GroupLockGuard lock(&groupsMutex);
    
    auto groupIt = groups.find(groupId);
    if (groupIt == groups.end()) {
        return false;
    }
    
    groupIt->second.removeConsumer(consumerId);
    consumerToGroup.erase(consumerId);
    
    // Trigger rebalance
    rebalanceGroup(groupId);
    
    std::cout << "[LEAVE_GROUP] Consumer " << consumerId 
              << " left group " << groupId << std::endl;
    
    return true;
}

bool GroupManager::heartbeat(const std::string& groupId, const std::string& consumerId) {
    GroupLockGuard lock(&groupsMutex);
    
    auto groupIt = groups.find(groupId);
    if (groupIt == groups.end()) {
        return false;
    }
    
    groupIt->second.updateHeartbeat(consumerId);
    
    std::cout << "[HEARTBEAT] Consumer " << consumerId 
              << " in group " << groupId << std::endl;
    
    return true;
}

std::vector<int> GroupManager::getConsumerPartitions(const std::string& groupId, 
                                                     const std::string& consumerId) {
    GroupLockGuard lock(&groupsMutex);
    
    std::vector<int> partitions;
    
    auto groupIt = groups.find(groupId);
    if (groupIt == groups.end()) {
        return partitions;
    }
    
    auto& group = groupIt->second;
    
    for (const auto& pair : group.partitionAssignments) {
        if (pair.second == consumerId) {
            partitions.push_back(pair.first);
        }
    }
    
    return partitions;
}

bool GroupManager::assignPartitions(const std::string& groupId, int partitionCount) {
    GroupLockGuard lock(&groupsMutex);
    
    auto groupIt = groups.find(groupId);
    if (groupIt == groups.end()) {
        return false;
    }
    
    auto& group = groupIt->second;
    
    // Clear existing assignments
    group.clearAssignments();
    
    // Get active consumers
    std::vector<std::string> activeConsumers = group.getActiveConsumers();
    
    if (activeConsumers.empty()) {
        return false;
    }
    
    // Assign partitions round-robin
    for (int p = 0; p < partitionCount; p++) {
        std::string consumer = activeConsumers[p % activeConsumers.size()];
        group.partitionAssignments[p] = consumer;
    }
    
    std::cout << "[ASSIGN_PARTITIONS] Group " << groupId 
              << " assigned " << partitionCount << " partitions to "
              << activeConsumers.size() << " consumers" << std::endl;
    
    return true;
}

std::string GroupManager::getConsumerForPartition(const std::string& groupId, int partition) {
    GroupLockGuard lock(&groupsMutex);
    
    auto groupIt = groups.find(groupId);
    if (groupIt == groups.end()) {
        return "";
    }
    
    auto& group = groupIt->second;
    auto it = group.partitionAssignments.find(partition);
    
    if (it != group.partitionAssignments.end()) {
        return it->second;
    }
    
    return "";
}

std::vector<std::string> GroupManager::getGroupConsumers(const std::string& groupId) {
    GroupLockGuard lock(&groupsMutex);
    
    auto groupIt = groups.find(groupId);
    if (groupIt == groups.end()) {
        return {};
    }
    
    return groupIt->second.getActiveConsumers();
}

bool GroupManager::isConsumerInGroup(const std::string& groupId, const std::string& consumerId) {
    GroupLockGuard lock(&groupsMutex);
    
    auto groupIt = groups.find(groupId);
    if (groupIt == groups.end()) {
        return false;
    }
    
    auto& group = groupIt->second;
    return group.consumers.find(consumerId) != group.consumers.end();
}

void GroupManager::cleanupDeadConsumers() {
    GroupLockGuard lock(&groupsMutex);
    
    auto now = std::chrono::steady_clock::now();
    
    for (auto& groupPair : groups) {
        auto& group = groupPair.second;
        std::vector<std::string> deadConsumers;
        
        for (auto& consumerPair : group.consumers) {
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                now - consumerPair.second.lastHeartbeat
            );
            
            if (elapsed.count() >= 30) {
                deadConsumers.push_back(consumerPair.first);
            }
        }
        
        for (const auto& consumerId : deadConsumers) {
            std::cout << "[CLEANUP] Removing dead consumer " << consumerId 
                      << " from group " << groupPair.first << std::endl;
            group.removeConsumer(consumerId);
            consumerToGroup.erase(consumerId);
        }
        
        if (!deadConsumers.empty()) {
            rebalanceGroup(groupPair.first);
        }
    }
}

bool GroupManager::groupExists(const std::string& groupId) const {
    return groups.find(groupId) != groups.end();
}

std::string GroupManager::getGroupTopic(const std::string& groupId) const {
    auto it = groups.find(groupId);
    if (it != groups.end()) {
        return it->second.topic;
    }
    return "";
}