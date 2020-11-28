#include "lockfreelist.hpp"
template<typename T>
LockFreeList<T>::LockFreeList() {
    this->tail = new node<T>(nullptr);
}

template<typename T>
void LockFreeList<T>::add(const T& data) {
    node<T>* new_node = new node<T>(data);
    auto prevTail = tail.load(std::memory_order_relaxed);
    do {
        new_node->prev = prevTail;
    } while (!tail.compare_exchange_weak(prevTail, new_node, std::memory_order_release, std::memory_order_relaxed));
}


template<typename T>
void LockFreeList<T>::destroy() {
    node<T>* current = tail.load(std::memory_order_acquire);
    node<T>* prev;
    while (current != nullptr)  { 
        prev = current->prev;
        delete current; 
        current = prev; 
    }
    this->tail=nullptr;
}

template<typename T>
void LockFreeList<T>::reset_tail() {
    if (this->tail == nullptr)
        this->tail = new node<T>(nullptr);
    return;
}
