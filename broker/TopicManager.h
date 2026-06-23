#ifndef TOPIC_MANAGER_H
#define TOPIC_MANAGER_H

#include <string>
#include <unordered_map>

#ifdef _WIN32
    #include <windows.h>
    typedef CRITICAL_SECTION PlatformMutex;
#else
    #include <mutex>
    typedef std::mutex PlatformMutex;
#endif

class TopicManager {
private:
    PlatformMutex topicMutex;
    std::unordered_map<std::string, int> nextOffsets; // Cache of next offsets for topics

    int getNextOffset(const std::string& topic); // Helper to get/initialize the next offset

public:
    TopicManager();
    ~TopicManager();
    bool createTopic(const std::string& topic);
    int appendMessage(const std::string& topic, const std::string& message); // Returns assigned offset, or -1 on error
    std::string getMessages(const std::string& topic, int afterOffset); // Returns records after the given offset

    bool commitOffset(const std::string& topic, const std::string& consumerId, long offset); // Persist consumer offset
    long getOffset(const std::string& topic, const std::string& consumerId); // Retrieve consumer offset
};

#endif // TOPIC_MANAGER_H
