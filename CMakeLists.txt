cmake_minimum_required(VERSION 3.10)
project(KafkaClone CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Broker Executable
add_executable(broker
    broker/main.cpp
    broker/Broker.cpp
    broker/Broker.h
    broker/TopicManager.cpp
    broker/TopicManager.h
    broker/ConsumerGroup.cpp      # NEW
    broker/ConsumerGroup.h        # NEW
    broker/GroupManager.cpp       # NEW
    broker/GroupManager.h  
)

# Producer Executable
add_executable(producer
    producer/main.cpp
    producer/Producer.cpp
    producer/Producer.h
)

# Consumer Executable
add_executable(consumer
    consumer/main.cpp
    consumer/Consumer.cpp
    consumer/Consumer.h
)

# Include directories for all targets
target_include_directories(broker PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/broker
)

target_include_directories(producer PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/producer
)

target_include_directories(consumer PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/consumer
)


if(WIN32)
    target_compile_definitions(broker PRIVATE _WIN32_WINNT=0x0600)
    target_compile_definitions(producer PRIVATE _WIN32_WINNT=0x0600)
    target_compile_definitions(consumer PRIVATE _WIN32_WINNT=0x0600)
    target_link_libraries(broker ws2_32)
    target_link_libraries(producer ws2_32)
    target_link_libraries(consumer ws2_32)
else()
    target_link_libraries(broker pthread)
    target_link_libraries(producer pthread)
    target_link_libraries(consumer pthread)

endif()
