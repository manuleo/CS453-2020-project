#include "help.hpp"


MemorySegment::MemorySegment(size_t size) {
    this->size = size;
    this->is_freed.store(false);
    return;
}

MemorySegment::~MemorySegment() {
    return;
}

Region::Region(size_t size, size_t align) {
    this->size = size;
    this->align = align;
    this->clock.store(0);
    this->tran_counter.store(0);
}

Region::~Region() {
    return;
}

WordLock::WordLock() {
    this->version.store(0);
    this->is_freed.store(false);
    return;
}

WordLock::~WordLock() {
    return;
}

TransactionObject::TransactionObject(uint t_id, bool is_ro, uint rv) {
    this->t_id = t_id;
    this->is_ro = is_ro;
    this->rv = rv;
    this->removed = false;
}

TransactionObject::~TransactionObject() {
    return;
}

Write::Write(shared_ptr<WordLock> lock, shared_ptr<MemorySegment> segment, WriteType type) {
    this->lock = lock;
    this->segment = segment;
    this->type = type;
    if (type == WriteType::alloc)
        this->allocated = false;
    else
        this->allocated = true;
    this->will_be_freed = false;
    this->data = nullptr;
}

Write::~Write() {
    return;
}