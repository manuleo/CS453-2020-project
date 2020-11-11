/**
 * @file   tm.c
 * @author Manuel Leone
 *
 * @section LICENSE
 *
 * GPL 3.0
 *
 * @section DESCRIPTION
 *
 * Implementation of your own transaction manager.
 * You can completely rewrite this file (and create more files) as you wish.
 * Only the interface (i.e. exported symbols and semantic) must be preserved.
**/

#include <help.hpp>
#include <tm.hpp>

// -------------------------------------------------------------------------- //
// Helper functions

void removeT(shared_ptr<TransactionObject> tran) {
    for (pair<void*, Write> write : tran->writes) {
        if (write.second.data != nullptr)
            free(write.second.data);
        if (write.second.type == WriteType::alloc) {
            shared_ptr<MemorySegment> seg = write.second.segment;
            free(seg->data);
            seg->writelocks.clear();
        }
    }
    tran->writes.clear();
    tran->order_writes.clear();
    tran->reads.clear();
    tran->allocated.clear();
    return;
}

// -------------------------------------------------------------------------- //

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align) noexcept {
    Region* reg = new Region(size, align);
    if (unlikely(!reg)) {
        return invalid_shared;
    }
    shared_ptr<MemorySegment> first = make_shared<MemorySegment>(size);
    if (unlikely(!first)) {
        free(reg);
        return invalid_shared;
    }
    if (unlikely(posix_memalign(&(first->data), align, size) != 0)) {
        free(reg);
        return invalid_shared;
    }
    memset(first->data, 0, size);
    void* start_segment = first->data;
    for (size_t i = 0; i < size; i+=align) {
        first->writelocks[start_segment+i] = make_shared<WordLock>();
    }
    uint idx = reg->mem_counter++;
    reg->memory[idx] = first;
    for (size_t i = 0; i < size; i+=align) {
        reg->memory_map[start_segment+i] = idx;
    }
    return reg;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) noexcept {
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) noexcept {
    return ((Region*)shared)->memory[1]->data;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) noexcept {
    return ((Region*)shared)->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared) noexcept {
    return ((Region*)shared)->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared, bool is_ro) noexcept {
    Region* reg = (Region*) shared;
    uint t_id = reg->tran_counter++;
    shared_ptr<TransactionObject> tran = make_shared<TransactionObject>(t_id, is_ro, reg->clock.load());
    if (unlikely(!tran)) {
        return invalid_tx;
    }
    reg->trans[t_id] = tran;
    return tran->t_id;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t tx) noexcept {

}

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read(shared_t shared, tx_t tx, void const* source, size_t size, void* target) noexcept {
    Region* reg = (Region*) shared;
    shared_ptr<TransactionObject> tran = reg->trans.at(tx);
    shared_ptr<MemorySegment> seg = nullptr;
    for (size_t i = 0; i < size; i+=reg->align) {
        void* word = const_cast<void*>(source) + i;
        if (!tran->is_ro) {
            if (tran->writes.count(word) == 1) {
                shared_ptr<WordLock> word_lock = tran->writes[word].lock;
                if (tran->reads.count(word_lock) == 0)
                    tran->reads.insert(word_lock);
                memcpy(target+i, tran->writes[word].data, reg->align);
            }
        }
        else {
            if (seg == nullptr) {
                reg->lock_mem.lock();
                if (reg->memory_map.count(word) == 1) {
                    seg = reg->memory.at(reg->memory_map.at(word));
                    reg->lock_mem.unlock();
                }
                else {
                    reg->lock_mem.unlock();
                    removeT(tran);
                    return false;
                }
            }
            seg->lock_pointers.lock();
            shared_ptr<WordLock> word_lock = seg->writelocks.at(word);
            seg->lock_pointers.unlock();
            uint write_ver = word_lock->version.load();
            // TODO: how does it work the post-validation here??? What they mean by "location’s versioned write-lock is free and has not changed"
            // should we check if we can have the lock too?
            // TODO: understand what bad can happen here with a freed segment
            memcpy(target+i, seg->data+i, reg->align);
            uint new_ver = word_lock->version.load();
            if (new_ver != write_ver) {
                removeT(tran);
                return false;
            }
            if (write_ver > tran->rv) {
                removeT(tran);
                return false;
            }
        }
    }
}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t shared, tx_t tx, void const* source, size_t size, void* target) noexcept {
    Region* reg = (Region*) shared;
    shared_ptr<TransactionObject> tran = reg->trans.at(tx);
    shared_ptr<MemorySegment> seg = nullptr;
    for (size_t i = 0; i < size; i+=reg->align) {
        void* word = target + i;
        if (tran->writes.count(word) == 1) {
            memcpy(tran->writes[word].data, source + i, reg->align);
            if (tran->writes[word].type==WriteType::dummy)
                tran->writes[word].type = WriteType::write;
        }
        else {
            if (seg == nullptr) {
                reg->lock_mem.lock();
                seg = reg->memory.at(reg->memory_map.at(word));
                reg->lock_mem.unlock();
            }
            seg->lock_pointers.lock();
            shared_ptr<WordLock> word_lock = seg->writelocks.at(word);
            seg->lock_pointers.unlock();
            tran->writes[word] = Write(word_lock, nullptr, WriteType::write);
            tran->writes[word].data = malloc(reg->align);
            memcpy(tran->writes[word].data, source + i, reg->align);
        }
        tran->order_writes.push_back(word);
    }
    // TODO: should we do the same checks as read here too?
    return true;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
Alloc tm_alloc(shared_t shared, tx_t tx, size_t size, void** target) noexcept {
    Region* reg = (Region*) shared;
    shared_ptr<TransactionObject> tran = reg->trans.at(tx);
    shared_ptr<MemorySegment> new_seg = make_shared<MemorySegment>(size);
    if (unlikely(!new_seg)) {
        return Alloc::nomem;
    }
    if (unlikely(posix_memalign(&(new_seg->data), reg->align, size) != 0)) {
        return Alloc::nomem;
    }
    memset(new_seg->data, 0, size);
    tran->writes[new_seg.get()] = Write(nullptr, new_seg, WriteType::alloc);
    tran->order_writes.push_back(new_seg.get());
    void* start_segment = new_seg->data;
    tran->allocated[start_segment] = new_seg;
    for (size_t i = 0; i < size; i+=reg->align) {
        new_seg->writelocks[start_segment+i] = make_shared<WordLock>();
        tran->writes[start_segment+i] = Write(new_seg->writelocks[start_segment+i], nullptr, WriteType::dummy);
    }
    *target = new_seg->data;
    return Alloc::success;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared, tx_t tx, void* target) noexcept {
    Region* reg = (Region*) shared;
    shared_ptr<MemorySegment> seg = nullptr;
    shared_ptr<TransactionObject> tran = reg->trans.at(tx);
    if (tran->allocated.count(target) == 1) {
        seg = tran->allocated[target];
    }
    else {
        reg->lock_mem.lock();
        seg = reg->memory.at(reg->memory_map.at(target));
        reg->lock_mem.unlock();
    }
    if (tran->writes.count(seg.get()) == 1) {
        tran->writes[seg.get()].type = WriteType::free;
    } 
    else {
        tran->writes[seg.get()] = Write(nullptr, seg, WriteType::free);
    }
    tran->order_writes.push_back(seg.get());
    return true;
}
