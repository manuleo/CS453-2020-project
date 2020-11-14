#include "help.hpp"

// -------------------------------------------------------------------------- //
// Lock implementation, that you have to complete

// NOTE: You may want to add data member(s) in 'class Lock' at entrypoint.hpp:30

/** Lock default constructor.
**/
// Lock::Lock() {
//     return;
// }

// /** Lock destructor.
// **/
// Lock::~Lock() {
//     return;
// }

// bool Lock::init() {
//     atomic_init(&locked, false);
//     return true;
// }

// /** [thread-safe] Acquire the lock, block if it is already acquired.
// **/
// bool Lock::lock() {
//     bool expected = false;
//     while (unlikely(!atomic_compare_exchange_weak_explicit(&locked, &expected, true, memory_order_acquire, memory_order_relaxed))) {
//         expected = false;
//         while (unlikely(atomic_load_explicit(&locked, memory_order_relaxed)))
//             pause();
//     }
//     return true;
// }

// /** [thread-safe] Release the lock, assuming it is indeed held by the caller.
// **/
// void Lock::unlock() {
//     atomic_store_explicit(&locked, false, memory_order_release);
//     return;
// }

MemoryObject::MemoryObject() {
    this->id_deleted = -1;
    return;
}

MemoryObject::~MemoryObject() {
    return;
}

Region::Region(size_t size, size_t align) {
    t_count = 0;
    this->size = size;
    this->align = align;
}

Region::~Region() {
    return;
}

TransactionObject::TransactionObject(int t_id, bool is_ro) {
    this->t_id = t_id;
    this->is_ro = is_ro;
}

TransactionObject::~TransactionObject() {
    return;
}

VersionTuple::VersionTuple(int ts, void* data) {
    this->data = data;
    this->ts = ts;
}

VersionTuple::~VersionTuple() {
    return;
}

Write::Write(shared_ptr<MemoryObject> object, size_t size, WriteType type) {
    this->object = object;
    this->size = size;
    this->type = type;
    this->read = false;
}

Write::~Write() {
    return;
}