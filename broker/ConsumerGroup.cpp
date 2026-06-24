// broker/ConsumerGroup.cpp
#include "ConsumerGroup.h"
#include <iostream>
#include <algorithm>

void ConsumerGroup::addConsumer(const std::string& consumerId) {
    auto it = consumers.find(consumerId);
    if (it == consumers.end()) {
        consumers[consumerId] = ConsumerInfo(consumerId);
    } else {
        it->second.active = true;
        it->second.lastHeartbeat = std::chrono::steady_clock::now();
    }
}

void ConsumerGroup::removeConsumer(const std::string& consumerId) {
    consumers.erase(consumerId);
    // Remove partition assignments for this consumer
    for (auto it = partitionAssignments.begin(); it != partitionAssignments.end();) {
        if (it->second == consumerId) {
            it = partitionAssignments.erase(it);
        } else {
            ++it;
        }
    }
}

void ConsumerGroup::updateHeartbeat(const std::string& consumerId) {
    auto it = consumers.find(consumerId);
    if (it != consumers.end()) {
        it->second.lastHeartbeat = std::chrono::steady_clock::now();
        it->second.active = true;
    }
}

int ConsumerGroup::getConsumerCount() const {
    return consumers.size();
}

std::vector<std::string> ConsumerGroup::getActiveConsumers() {
    std::vector<std::string> active;
    auto now = std::chrono::steady_clock::now();
    
    for (auto& pair : consumers) {
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            now - pair.second.lastHeartbeat
        );
        
        // Consider consumer dead if no heartbeat for 30 seconds
        if (elapsed.count() < 30) {
            active.push_back(pair.first);
        }
    }
    return active;
}

void ConsumerGroup::clearAssignments() {
    partitionAssignments.clear();
}