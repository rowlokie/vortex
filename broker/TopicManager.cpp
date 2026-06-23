#include "TopicManager.h"
#include <fstream>
#include <iostream>
#include <ctime>

#ifdef _WIN32
#include <direct.h>
#define make_dir(dir) _mkdir(dir)
#else
#include <sys/stat.h>
#include <sys/types.h>
#define make_dir(dir) mkdir(dir, 0777)
#endif

class LogLockGuard {
    PlatformMutex* mtx;
public:
    LogLockGuard(PlatformMutex* m) : mtx(m) {
#ifdef _WIN32
        EnterCriticalSection(mtx);
#else
        mtx->lock();
#endif
    }
    ~LogLockGuard() {
#ifdef _WIN32
        LeaveCriticalSection(mtx);
#else
        mtx->unlock();
#endif
    }
};

TopicManager::TopicManager() {
#ifdef _WIN32
    InitializeCriticalSection(&topicMutex);
#endif
    make_dir("data");
    make_dir("data/offsets");
}

TopicManager::~TopicManager() {
#ifdef _WIN32
    DeleteCriticalSection(&topicMutex);
#endif
}

int TopicManager::getNextOffset(const std::string& topic) {
    auto it = nextOffsets.find(topic);
    if (it != nextOffsets.end()) {
        return it->second;
    }

    int count = 0;
    std::string path = "data/" + topic + ".log";
    std::ifstream file(path);
    if (file.is_open()) {
        std::string line;
        while (std::getline(file, line)) {
            count++;
        }
        file.close();
    }
    int nextVal = count + 1;
    nextOffsets[topic] = nextVal;
    return nextVal;
}

bool TopicManager::createTopic(const std::string& topic) {
    if (topic.empty() || topic.find("..") != std::string::npos || topic.find('/') != std::string::npos || topic.find('\\') != std::string::npos) {
        return false;
    }

    LogLockGuard lock(&topicMutex);
    std::string path = "data/" + topic + ".log";
    std::ofstream file(path, std::ios::app);
    if (file.good()) {
        getNextOffset(topic);
        return true;
    }
    return false;
}

int TopicManager::appendMessage(const std::string& topic, const std::string& message) {
    if (topic.empty() || topic.find("..") != std::string::npos || topic.find('/') != std::string::npos || topic.find('\\') != std::string::npos) {
        return -1;
    }

    LogLockGuard lock(&topicMutex);

    int offset = getNextOffset(topic);
    std::string path = "data/" + topic + ".log";
    std::ofstream file(path, std::ios::app);
    if (!file.is_open()) {
        return -1;
    }

    std::time_t ts = std::time(nullptr);
    file << offset << "|" << ts << "|" << message << "\n";
    
    nextOffsets[topic] = offset + 1;

    return offset;
}

std::string TopicManager::getMessages(const std::string& topic, int afterOffset) {
    if (topic.empty() || topic.find("..") != std::string::npos || topic.find('/') != std::string::npos || topic.find('\\') != std::string::npos) {
        return "";
    }

    LogLockGuard lock(&topicMutex);
    std::string path = "data/" + topic + ".log";
    std::ifstream file(path);
    std::string result;
    if (file.is_open()) {
        std::string line;
        while (std::getline(file, line)) {
            size_t firstPipe = line.find('|');
            if (firstPipe != std::string::npos) {
                try {
                    int offset = std::stoi(line.substr(0, firstPipe));
                    if (offset > afterOffset) {
                        result += line + "\n";
                    }
                } catch (...) {
                    // Skip malformed lines
                }
            }
        }
        file.close();
    }
    return result;
}

bool TopicManager::commitOffset(const std::string& topic, const std::string& consumerId, long offset) {
    if (topic.empty() || consumerId.empty() || topic.find("..") != std::string::npos || consumerId.find("..") != std::string::npos) {
        return false;
    }

    LogLockGuard lock(&topicMutex);
    std::string path = "data/offsets/" + topic + "_" + consumerId + ".offset";
    std::ofstream file(path, std::ios::trunc); // Overwrite existing offset
    if (!file.is_open()) {
        return false;
    }
    file << offset << "\n";
    return file.good();
}

long TopicManager::getOffset(const std::string& topic, const std::string& consumerId) {
    if (topic.empty() || consumerId.empty() || topic.find("..") != std::string::npos || consumerId.find("..") != std::string::npos) {
        return 0;
    }

    LogLockGuard lock(&topicMutex);
    std::string path = "data/offsets/" + topic + "_" + consumerId + ".offset";
    std::ifstream file(path);
    if (!file.is_open()) {
        return 0;
    }
    long offset = 0;
    if (file >> offset) {
        file.close();
        return offset;
    }
    file.close();
    return 0;
}
