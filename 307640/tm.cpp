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

void removeT(Transaction* tran, bool failed) {
    if (unlikely(tran->alloc_size.size()!=0)) {
        if (unlikely(failed)) {
            for (auto const& pair: tran->alloc_size)
                free(pair.first);
            for (auto const& pair: tran->allocated)
                delete pair.second.second;
        }
    }
    if (unlikely(failed)) {
        for (auto const& write: tran->writes) {
            // write->written.store(false);
            // write->access.store(-1);
            write->read_tran.store(-1);
            write->write_tran.store(-1);
        }
    }
    tran->alloc_size.clear();
    tran->allocated.clear();
    tran->frees.clear();
    tran->writes.clear();
    delete tran;
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
    void* first;
    if (unlikely(posix_memalign(&first, align, size*2) != 0)) {
        free(reg);
        return invalid_shared;
    }
    memset(first, 0, size);
    void* iter_word = first;
    for (size_t i = 0; i < size; i+=align) {
        reg->memory[first+i] = make_pair(iter_word, new WordControl());
        iter_word = iter_word + 2*align;
    }
    reg->first_word = first;
    reg->memory_sizes[first] = size;
    return reg;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) noexcept {
    Region* reg = (Region*) shared;
    for (auto const& pair: reg->memory) {
        delete pair.second.second;
    }
    reg->memory.clear();
    for (auto const& pair: reg->memory_sizes) {
        free(pair.first);
    }
    reg->memory_sizes.clear();
    delete reg->batcher;
    delete reg;
    return;
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) noexcept {
    return ((Region*)shared)->first_word;
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
    Transaction* tran = new Transaction(t_id, is_ro);
    if (unlikely(!tran)) {
        return invalid_tx;
    }
    reg->batcher->enter();
    return reinterpret_cast<tx_t>(tran);
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t tx) noexcept {
    Region* reg = (Region*) shared;
    Transaction* tran = reinterpret_cast<Transaction*>(tx);
    if (unlikely(!tran->is_ro)) {
        if (unlikely(tran->allocated.size()!=0)) {
            unique_lock<mutex> lock_allocation{reg->lock_alloc};
            for (auto const& pair: tran->allocated)
                reg->to_allocate[pair.first] = pair.second;
            for (auto const& pair: tran->alloc_size)
                reg->memory_sizes[pair.first] = pair.second;
            lock_allocation.unlock();
        }
        if (unlikely(tran->frees.size()!=0)) {
            unique_lock<mutex> lock_frees{reg->lock_free};
            for (auto const& word: tran->frees)
                reg->to_free.push_back(word); 
            lock_frees.unlock();
        }
        if (likely(tran->writes.size()!=0)) {
            for (auto const& write: tran->writes)
                write->commit_write.store(true);
        }
    }
    // if (tran->writes.size() == 1) {
    //     cout << "Only 1 write in a short_tx!!!!" << endl;
    // }
    reg->batcher->leave();
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
    Transaction* tran = reinterpret_cast<Transaction*>(tx);
    for (size_t i = 0; i < size; i+=reg->align) {
        void* word = const_cast<void*>(source) + i;
        void* new_target = target + i;
        pair<void*, WordControl*> word_struct;
        if (unlikely(reg->memory.count(word)!=1))
            word_struct = tran->allocated.at(word);
        else
            word_struct = reg->memory.at(word);
        void* read_copy = word_struct.first + (word_struct.second->read_version ? reg->align : 0);
        if (likely(tran->is_ro)) {
            //word_struct.second->access.store(tran->t_id);
            memcpy(new_target, read_copy, reg->align);
            continue;
        }
        // TODO: check if we need the lock
        //shared_lock<shared_mutex> write_check_lock{word_struct.second->lock_write};
        void* write_copy = word_struct.first + (word_struct.second->read_version ? 0 : reg->align);
        // if (likely(word_struct.second->written.load())) {
        //     if (likely(word_struct.second->access.load() == tran->t_id)) {
        //         memcpy(new_target, write_copy, reg->align);
        //         continue;
        //     } else {
        //         reg->batcher->leave();
        //         removeT(tran, true);
        //         return false;
        //     }
        // } else {
        //     word_struct.second->access.store(tran->t_id);
        //     memcpy(new_target, read_copy, reg->align);
        //     continue;
        // }
        if (likely(word_struct.second->write_tran.load() == tran->t_id)) {
            word_struct.second->read_tran.store(tran->t_id);
            memcpy(new_target, write_copy, reg->align);
            continue;
        } else if (likely(word_struct.second->write_tran.load() == -1)) {
            // TODO: check if we need a lock shared here just before the copy 
            // (avoiding someone to start writing on the word after we checked the -1)
            memcpy(new_target, read_copy, reg->align);
            word_struct.second->read_tran.store(tran->t_id);
        } else {
            removeT(tran, true);
            reg->batcher->leave();
            return false;
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
    Transaction* tran = reinterpret_cast<Transaction*>(tx);
    for (size_t i = 0; i < size; i+=reg->align) {
        void* word = target + i;
        void* new_source = const_cast<void*>(source) + i;
        pair<void*, WordControl*> word_struct;
        if (unlikely(reg->memory.count(word)!=1))
            word_struct = tran->allocated.at(word);
        else
            word_struct = reg->memory.at(word);
        void* write_copy = word_struct.first + (word_struct.second->read_version ? 0 : reg->align);
        int expected_tran = -1;
        bool expected_written = false;
        // if (unlikely(word_struct.second->written.load())) {
        //     if (unlikely(word_struct.second->access.load() == tran->t_id)){
        //         memcpy(write_copy, new_source, reg->align);
        //         tran->writes.push_back(word_struct.second);
        //         continue;
        //     } else {
        //         reg->batcher->leave();
        //         removeT(tran, true);
        //         return false;
        //     }
        // } else {
        //     if (unlikely(word_struct.second->access.load() != tran->t_id && word_struct.second->access.load() != -1)){
        //         reg->batcher->leave();
        //         removeT(tran, true);
        //         return false;
        //     } else {
        //         word_struct.second->written.store(true);
        //         word_struct.second->access.store(tran->t_id);
        //         memcpy(write_copy, new_source, reg->align);
        //         tran->writes.push_back(word_struct.second);
        //         continue;
        //     }
        // }
        // if (likely(word_struct.second->written.compare_exchange_strong(expected_written, true))) {
        //     if (likely(word_struct.second->access.compare_exchange_weak(expected_tran, tran->t_id))) {
        //         memcpy(write_copy, new_source, reg->align);
        //         tran->writes.push_back(word_struct.second);
        //         continue;
        //     } else if (likely(word_struct.second->access.load() == tran->t_id)) {
        //         memcpy(write_copy, new_source, reg->align);
        //         tran->writes.push_back(word_struct.second);
        //         continue;
        //     }
        //     else {
        //         word_struct.second->written.store(false);
        //         reg->batcher->leave();
        //         removeT(tran, true);
        //         return false;
        //     }
        // } else if (unlikely(word_struct.second->access.load() == tran->t_id)){
        //     memcpy(write_copy, new_source, reg->align);
        //     tran->writes.push_back(word_struct.second);
        //     continue;
        // } else {
        //     reg->batcher->leave();
        //     removeT(tran, true);
        //     return false;
        // }
        int expected = -1;
        if (likely(word_struct.second->write_tran.compare_exchange_weak(expected, tran->t_id))) {
            if (unlikely(word_struct.second->read_tran.compare_exchange_weak(expected, tran->t_id))) {
                word_struct.second->write_tran.store(tran->t_id);
                memcpy(write_copy, new_source, reg->align);
                tran->writes.push_back(word_struct.second);
                continue;
            } else if (unlikely(expected == tran->t_id)) {
                word_struct.second->write_tran.store(tran->t_id);
                memcpy(write_copy, new_source, reg->align);
                tran->writes.push_back(word_struct.second);
                continue;
            }
            else {
                removeT(tran, true);
                reg->batcher->leave();
                return false;
            }
            // // May be not needed, a read would check just the write and another write would abort anyway at the write check
            // // word_struct.second->read_tran.store(tran->t_id);
            // memcpy(write_copy, target+i, reg->align);
            // continue;
        } else if (unlikely(expected == tran->t_id)) {
            memcpy(write_copy, new_source, reg->align);
            continue;
        } else {
            removeT(tran, true);
            reg->batcher->leave();
            return false;
        }
    }
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
    Transaction* tran = reinterpret_cast<Transaction*>(tx);
    void* new_seg;
    if (unlikely(posix_memalign(&new_seg, reg->align, size*2) != 0)) {
        return Alloc::nomem;
    }
    memset(new_seg, 0, size);
    tran->alloc_size.push_back(make_pair(new_seg, size));
    void* iter_word = new_seg;
    for (size_t i = 0; i < size; i+=reg->align) {
        tran->allocated[new_seg+i] = make_pair(iter_word, new WordControl());
        iter_word = iter_word + 2*reg->align;
    }
    *target = new_seg;
    return Alloc::success;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared as (unused), tx_t tx, void* target) noexcept {
    Transaction* tran = reinterpret_cast<Transaction*>(tx);
    tran->frees.push_back(target);
    return true;
}