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

if(WIN32)
    target_compile_definitions(broker PRIVATE _WIN32_WINNT=0x0600)
    target_compile_definitions(producer PRIVATE _WIN32_WINNT=0x0600)
    target_compile_definitions(consumer PRIVATE _WIN32_WINNT=0x0600)
    target_link_libraries(broker ws2_32)
    target_link_libraries(producer ws2_32)
    target_link_libraries(consumer ws2_32)
endif()
