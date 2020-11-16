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

void removeT(shared_ptr<TransactionObject> tran, bool failed) {
    for (auto& write : tran->writes) {
        if (write.second->data != nullptr)
            free(write.second->data);
        if (write.second->type == WriteType::alloc) {
            shared_ptr<MemorySegment> seg = write.second->segment;
            if (failed) {
                free(seg->data);
                seg->writelocks.clear();
            }
        }
        if (write.second->type == WriteType::free) {
            write.second->lock_frees.clear();
        }
        delete write.second;
    }
    tran->writes.clear();
    tran->order_writes.clear();
    tran->reads.clear();
    tran->allocated.clear();
    tran->removed = true;
    return;
}

void freeLocks(map<void*,list<unique_lock<recursive_timed_mutex>*>>* acq_locks) {
    for (auto const& pair : *acq_locks) {
        for (auto const& lock: pair.second) {
            lock->unlock();
            delete lock;
        }
    }
    acq_locks->clear();
    return;
}

void cleanSeg(shared_ptr<MemorySegment> seg) {
    seg->lock_pointers.lock();
    seg->writelocks.clear();
    free(seg->data);
    seg->lock_pointers.unlock();
    return;
}

bool validateReads(shared_ptr<TransactionObject> tran) {
    // TODO: understand what "We also verify that these memory locations have not been locked by other threads" means
    // Should we lock read locations too?
    for (auto &read : tran->reads) {
        if (read.first->version > tran->rv) {
            if (read.first->is_freed.load()) {
                if (read.second.unique())
                    cleanSeg(read.second);
                else
                    read.second.reset();
            }
            removeT(tran, true);
            return false;
        }
    }
    return true;
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
    uint idx = ++reg->mem_counter;
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
    Region* reg = (Region*) shared;
    for (auto &pair_seg: reg->memory) {
        if (!pair_seg.second->is_freed.load()) {
            cleanSeg(pair_seg.second);
        }
    }
    reg->memory.clear();
    reg->memory_map.clear();
    for (auto &pair_tran: reg->trans) {
        if (!pair_tran.second->removed) {
            removeT(pair_tran.second, false);
        }
    }
    reg->trans.clear();
    delete reg;
    return;
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
    uint t_id = ++reg->tran_counter;
    shared_ptr<TransactionObject> tran = make_shared<TransactionObject>(t_id, is_ro, reg->clock.load());
    if (unlikely(!tran)) {
        return invalid_tx;
    }
    reg->lock_trans.lock();
    reg->trans[t_id] = tran;
    reg->lock_trans.unlock();
    return tran->t_id;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t tx) noexcept {
    Region* reg = (Region*) shared;
    reg->lock_trans.lock();
    shared_ptr<TransactionObject> tran = reg->trans.at(tx);
    reg->lock_trans.unlock();
    if (tran->is_ro) {
        if (!validateReads(tran)) {
            removeT(tran, true);
            return false;
        }
        else {
            removeT(tran, false);
            return true;
        }
    }
    chrono::milliseconds try_dur(100);
    map<void*, list<unique_lock<recursive_timed_mutex>*>> acq_locks;
    for (auto &write : tran->writes) {
        if (write.second->type == WriteType::write || write.second->type == WriteType::dummy) {
            acq_locks[write.first].push_back(new unique_lock<recursive_timed_mutex>(write.second->lock->lock, defer_lock));
            if (!(acq_locks[write.first].back()->try_lock_for(try_dur))) {
                removeT(tran, true);
                freeLocks(&acq_locks);
                return false;
            }
            if (write.second->will_be_freed) 
                acq_locks[write.first].push_back(new unique_lock<recursive_timed_mutex>(write.second->lock->lock));
        }
    }
    tran->wv = ++reg->clock;
    if (tran->rv + 1u != tran->wv) {
        if (!validateReads(tran)) {
            freeLocks(&acq_locks);
            return false;
        }
    }
    // Validating write and frees w.r.t other possible free before proceeding
    for (auto &write : tran->writes) {
        if (write.second->type == WriteType::write || write.second->type == WriteType::free) {
            if (write.second->segment->is_freed.load()) {
                if (write.second->segment.unique())
                    cleanSeg(write.second->segment);
                else
                    write.second->segment.reset();
                removeT(tran, true);
                freeLocks(&acq_locks);
                return false;
            }
        }
    }
    // Now we are sure we can commit
    for (void* addr: tran->order_writes) {
        Write* w = tran->writes[addr];
        if (w->type == WriteType::write) {
            memcpy(addr, w->data, reg->align);
            w->lock->version.store(tran->wv);
            acq_locks[addr].front()->unlock();
            delete acq_locks[addr].front();
            acq_locks[addr].pop_front();
            if (acq_locks[addr].size() == 0)
                acq_locks.erase(addr);
        }
        else if (w->type == WriteType::alloc) {
            void* start_segment = w->segment->data;
            reg->lock_mem.lock();
            uint idx = ++reg->mem_counter;
            reg->memory[idx] = w->segment;
            for (size_t i = 0; i < w->segment->size; i+=reg->align) {
                reg->memory_map[start_segment+i] = idx;
            }
            reg->lock_mem.unlock();
        }
        else if (w->type == WriteType::dummy) {
            w->lock->version.store(tran->wv);
            acq_locks[addr].front()->unlock();
            delete acq_locks[addr].front();
            acq_locks[addr].pop_front();
            if (acq_locks[addr].size() == 0)
                acq_locks.erase(addr);
        }
        else if (w->type == WriteType::free) {
            void* start_segment = w->segment->data;
            if (w->allocated) {
                // Segment was allocated (either by the transaction itself or by someone else before)
                reg->lock_mem.lock();
                uint idx = reg->memory_map.at(start_segment);
                reg->memory.erase(idx);
                for (size_t i = 0; i < w->segment->size; i+=reg->align) {
                    reg->memory_map.erase(start_segment+i);
                }
                reg->lock_mem.unlock();
                w->segment->is_freed.store(true);
                for (auto& free_lock: w->lock_frees) {
                    free_lock->version.store(tran->wv);
                    free_lock->is_freed.store(true);
                }
                for (size_t i = 0; i < w->segment->size; i+=reg->align) {
                    acq_locks[start_segment + i].front()->unlock();
                    delete acq_locks[start_segment + i].front();
                    acq_locks[start_segment + i].pop_front();
                    if (acq_locks[start_segment + i].size() == 0)
                        acq_locks.erase(start_segment + i);
                }
                if (w->segment.unique())
                    cleanSeg(w->segment);
                else
                    w->segment.reset();
            }
            else {
                // Segment wasn't allocated, we're first allocating then we'll free it (next time we encounter to do the free)
                reg->lock_mem.lock();
                uint idx = ++reg->mem_counter;
                reg->memory[idx] = w->segment;
                for (size_t i = 0; i < w->segment->size; i+=reg->align) {
                    reg->memory_map[start_segment+i] = idx;
                }
                reg->lock_mem.unlock();
                w->allocated = true;
            }
        }
    }
    removeT(tran, false);
    return true;
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
    reg->lock_trans.lock();
    shared_ptr<TransactionObject> tran = reg->trans.at(tx);
    reg->lock_trans.unlock();
    shared_ptr<MemorySegment> seg = nullptr;
    for (size_t i = 0; i < size; i+=reg->align) {
        void* word = const_cast<void*>(source) + i;
        bool taken_from_write = false;
        if (!tran->is_ro) {
            if (tran->writes.count(word) == 1) {
                shared_ptr<WordLock> word_lock = tran->writes[word]->lock;
                shared_ptr<MemorySegment> word_seg = tran->writes[word]->segment;
                pair<shared_ptr<WordLock>, shared_ptr<MemorySegment>> seg_word_pair = make_pair(word_lock, word_seg);
                if (none_of(tran->reads.begin(), tran->reads.end(), [&seg_word_pair](pair<shared_ptr<WordLock>, shared_ptr<MemorySegment>> const& elem) { return seg_word_pair == elem; }))
                    tran->reads.push_back(seg_word_pair);
                memcpy(target+i, tran->writes[word]->data, reg->align);
                taken_from_write = true;
            }
        }
        if (!taken_from_write){
            if (seg == nullptr) {
                reg->lock_mem.lock();
                if (reg->memory_map.count(word) == 1) {
                    seg = reg->memory.at(reg->memory_map.at(word));
                    reg->lock_mem.unlock();
                }
                else {
                    reg->lock_mem.unlock();
                    removeT(tran, true);
                    return false;
                }
            }
            seg->lock_pointers.lock();
            shared_ptr<WordLock> word_lock = seg->writelocks.at(word);
            seg->lock_pointers.unlock();
            uint write_ver = word_lock->version.load();
            // TODO: how does it work the post-validation here??? What they mean by "location’s versioned write-lock is free and has not changed"
            // should we check if we can have the lock too?
            // TODO: understand what bad can happen here with a freed segment: 
            // we can avoid to free the segment and wait until we have only one reference left?
            memcpy(target+i, word, reg->align);
            uint new_ver = word_lock->version.load();
            if (new_ver != write_ver) {
                removeT(tran, true);
                return false;
            }
            if (write_ver > tran->rv) {
                removeT(tran, true);
                return false;
            }
            pair<shared_ptr<WordLock>, shared_ptr<MemorySegment>> seg_word_pair = make_pair(word_lock, seg);
            if (none_of(tran->reads.begin(), tran->reads.end(), [&seg_word_pair](pair<shared_ptr<WordLock>, shared_ptr<MemorySegment>> const& elem) { return seg_word_pair == elem; }))
                tran->reads.push_back(seg_word_pair);
        }
    }
    return true;
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
    reg->lock_trans.lock();
    shared_ptr<TransactionObject> tran = reg->trans.at(tx);
    reg->lock_trans.unlock();
    shared_ptr<MemorySegment> seg = nullptr;
    for (size_t i = 0; i < size; i+=reg->align) {
        void* word = target + i;
        if (tran->writes.count(word) == 1) {
            memcpy(tran->writes[word]->data, source + i, reg->align);
            if (tran->writes[word]->type==WriteType::dummy)
                tran->writes[word]->type = WriteType::write;
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
                    removeT(tran, true);
                    return false;
                }
            }
            seg->lock_pointers.lock();
            shared_ptr<WordLock> word_lock = seg->writelocks.at(word);
            seg->lock_pointers.unlock();
            tran->writes[word] = new Write(word_lock, seg, WriteType::write);
            tran->writes[word]->data = malloc(reg->align);
            memcpy(tran->writes[word]->data, source + i, reg->align);
        }
        if (none_of(tran->order_writes.begin(), tran->order_writes.end(), [&word](void* const& elem) { return word == elem; }))
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
    reg->lock_trans.lock();
    shared_ptr<TransactionObject> tran = reg->trans.at(tx);
    reg->lock_trans.unlock();
    shared_ptr<MemorySegment> new_seg = make_shared<MemorySegment>(size);
    if (unlikely(!new_seg)) {
        return Alloc::nomem;
    }
    if (unlikely(posix_memalign(&(new_seg->data), reg->align, size) != 0)) {
        return Alloc::nomem;
    }
    memset(new_seg->data, 0, size);
    tran->writes[new_seg.get()] = new Write(nullptr, new_seg, WriteType::alloc);
    tran->order_writes.push_back(new_seg.get());
    void* start_segment = new_seg->data;
    tran->allocated[start_segment] = new_seg;
    for (size_t i = 0; i < size; i+=reg->align) {
        new_seg->writelocks[start_segment+i] = make_shared<WordLock>();
        tran->writes[start_segment+i] = new Write(new_seg->writelocks[start_segment+i], new_seg, WriteType::dummy);
        tran->writes[start_segment+i]->data = malloc(reg->align);
        tran->order_writes.push_back(start_segment+i);
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
    reg->lock_trans.lock();
    shared_ptr<TransactionObject> tran = reg->trans.at(tx);
    reg->lock_trans.unlock();
    if (tran->allocated.count(target) == 1) {
        seg = tran->allocated[target];
        tran->writes[seg.get()]->type = WriteType::free;
    }
    else {
        reg->lock_mem.lock();
        seg = reg->memory.at(reg->memory_map.at(target));
        reg->lock_mem.unlock();
        tran->writes[seg.get()] = new Write(nullptr, seg, WriteType::free);
    }
    seg->lock_pointers.lock();
    for (pair<void*, shared_ptr<WordLock>> wordlock: seg->writelocks) {
        if (tran->writes.count(wordlock.first)!=1) {
            tran->writes[wordlock.first] = new Write(wordlock.second, seg, WriteType::dummy);
        }
        tran->writes[wordlock.first]->will_be_freed = true;
        tran->writes[seg.get()]->lock_frees.push_back(wordlock.second);
    }
    seg->lock_pointers.unlock();
    tran->order_writes.push_back(seg.get());
    return true;
}
