// tests/test_consumer_groups.cpp
#include "../broker/TopicManager.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>

void testConsumerGroups() {
    std::cout << "=== Testing Consumer Groups ===" << std::endl;
    
    TopicManager tm;
    
    // 1. Create topic with 3 partitions
    std::cout << "\n1. Creating topic 'orders' with 3 partitions..." << std::endl;
    bool created = tm.createTopic("orders", 3);
    if (created) {
        std::cout << "✅ Topic created successfully!" << std::endl;
    } else {
        std::cout << "❌ Failed to create topic" << std::endl;
        return;
    }
    
    // 2. Join consumers to group
    std::cout << "\n2. Joining consumers to group 'analytics'..." << std::endl;
    
    bool joinA = tm.joinGroup("analytics", "orders", "consumerA");
    bool joinB = tm.joinGroup("analytics", "orders", "consumerB");
    bool joinC = tm.joinGroup("analytics", "orders", "consumerC");
    
    std::cout << "consumerA joined: " << (joinA ? "✅" : "❌") << std::endl;
    std::cout << "consumerB joined: " << (joinB ? "✅" : "❌") << std::endl;
    std::cout << "consumerC joined: " << (joinC ? "✅" : "❌") << std::endl;
    
    // 3. Check assignments
    std::cout << "\n3. Checking partition assignments..." << std::endl;
    
    auto partitionsA = tm.getConsumerPartitions("analytics", "consumerA");
    auto partitionsB = tm.getConsumerPartitions("analytics", "consumerB");
    auto partitionsC = tm.getConsumerPartitions("analytics", "consumerC");
    
    std::cout << "consumerA -> partitions: ";
    for (int p : partitionsA) std::cout << p << " ";
    std::cout << std::endl;
    
    std::cout << "consumerB -> partitions: ";
    for (int p : partitionsB) std::cout << p << " ";
    std::cout << std::endl;
    
    std::cout << "consumerC -> partitions: ";
    for (int p : partitionsC) std::cout << p << " ";
    std::cout << std::endl;
    
    // 4. Produce messages
    std::cout << "\n4. Producing 6 messages..." << std::endl;
    for (int i = 1; i <= 6; i++) {
        int partitionId = -1;
        long offset = tm.appendMessage("orders", "", "Message " + std::to_string(i), partitionId);
        if (offset != -1) {
            std::cout << "Message " << i << " -> partition " << partitionId 
                      << ", offset " << offset << std::endl;
        } else {
            std::cout << "❌ Failed to produce message " << i << std::endl;
        }
    }
    
    // 5. Poll each consumer
    std::cout << "\n5. Polling consumers..." << std::endl;
    
    std::cout << "\nconsumerA received:" << std::endl;
    std::string messagesA = tm.getMessagesForConsumer("analytics", "consumerA");
    std::cout << messagesA << std::endl;
    
    std::cout << "\nconsumerB received:" << std::endl;
    std::string messagesB = tm.getMessagesForConsumer("analytics", "consumerB");
    std::cout << messagesB << std::endl;
    
    std::cout << "\nconsumerC received:" << std::endl;
    std::string messagesC = tm.getMessagesForConsumer("analytics", "consumerC");
    std::cout << messagesC << std::endl;
    
    // 6. Test rebalancing
    std::cout << "\n6. Testing rebalancing - adding consumerD..." << std::endl;
    bool joinD = tm.joinGroup("analytics", "orders", "consumerD");
    std::cout << "consumerD joined: " << (joinD ? "✅" : "❌") << std::endl;
    
    auto partitionsD = tm.getConsumerPartitions("analytics", "consumerD");
    std::cout << "consumerD -> partitions: ";
    for (int p : partitionsD) std::cout << p << " ";
    std::cout << std::endl;
    
    // Check reassignments
    partitionsA = tm.getConsumerPartitions("analytics", "consumerA");
    std::cout << "consumerA now -> partitions: ";
    for (int p : partitionsA) std::cout << p << " ";
    std::cout << std::endl;
    
    std::cout << "\n=== Test Complete ===" << std::endl;
}

int main() {
    // Create data directories
    system("mkdir -p data");
    system("mkdir -p data/meta");
    system("mkdir -p data/offsets");
    
    testConsumerGroups();
    return 0;
}