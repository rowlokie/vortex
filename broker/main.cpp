#include "Broker.h"
#include <iostream>

int main() {
    Broker broker(9092);
    if (!broker.start()) {
        std::cerr << "Failed to start the broker.\n";
        return 1;
    }
    return 0;
}
