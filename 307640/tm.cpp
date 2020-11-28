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

// -------------------------------------------------------------------------- //

// atomic_int64_t tot_read_dur;
// atomic_int64_t tot_write_dur;
// atomic_int64_t tot_leave_dur;
// atomic_int64_t tot_enter_dur;
// atomic_int64_t tot_end_dur;
// atomic_uint tot_abort_read;
// atomic_uint tot_abort_write;

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
    void* first = calloc(size*2/reg->align, align);
    if (unlikely(first == nullptr)) {
        delete reg;
        return invalid_shared;
    }
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
    reg->written.destroy();
    delete reg->batcher;
    delete reg;

    // cout << "TOT end: " << (float) tot_end_dur << endl;
    // cout << "TOT read: " << (float) tot_read_dur << endl;
    // cout << "TOT write: " << (float) tot_write_dur << endl;
    // cout << "TOT enter: " << (float) tot_enter_dur << endl;
    // cout << "TOT leave: " << (float) tot_end_dur << endl;
    // cout << "TOT abort read: " << tot_abort_read << endl;
    // cout << "TOT abort write: " << tot_abort_write << endl; 
    // cout << "TOT end batch call: " << reg->count_end << endl;
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
    //std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    reg->batcher->enter();
    // std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    // int64_t dur = std::chrono::duration_cast<std::chrono::milliseconds> (end - begin).count();
    //tot_enter_dur+=dur;
    return reinterpret_cast<tx_t>(tran);
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t tx) noexcept {
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    Region* reg = (Region*) shared;
    Transaction* tran = reinterpret_cast<Transaction*>(tx);
    if (unlikely(!tran->is_ro)) {
        // if (unlikely(tran->allocated.size()!=0)) {
        //     unique_lock<mutex> lock_allocation{reg->lock_alloc};
        //     for (auto const& pair: tran->allocated)
        //         reg->to_allocate[pair.first] = pair.second;
        //     for (auto const& pair: tran->alloc_size)
        //         reg->memory_sizes[pair.first] = pair.second;
        //     lock_allocation.unlock();
        // }
        // if (unlikely(tran->frees.size()!=0)) {
        //     unique_lock<mutex> lock_frees{reg->lock_free};
        //     for (auto const& word: tran->frees)
        //         reg->to_free.push_back(word); 
        //     lock_frees.unlock();
        // }
        // if (likely(tran->writes.size()!=0)) {
        //     unique_lock<mutex> lock_write{reg->lock_written};
        //     for (auto const& write: tran->writes)
        //         reg->written.push_back(write);
        //     lock_write.unlock();
        // }
        // if (unlikely(tran->allocated.size()!=0)) {
        //     for (auto const& pair: tran->allocated)
        //         reg->to_allocate.add(make_pair(pair.first, pair.second));
        //     for (auto const& pair: tran->alloc_size)
        //         reg->new_sizes.add(pair);
        // }
        if (unlikely(tran->frees.size()!=0)) {
            for (auto const& word: tran->frees)
                reg->to_free.add(word);
        }
        if (likely(tran->writes.size()!=0)) {
            for (auto const& write: tran->writes)
                reg->written.add(write);
        }
    }
    //std::chrono::steady_clock::time_point begin_leave = std::chrono::steady_clock::now();
    reg->batcher->leave();
    // std::chrono::steady_clock::time_point end_leave = std::chrono::steady_clock::now();
    // int64_t dur_leave = std::chrono::duration_cast<std::chrono::milliseconds> (end_leave - begin_leave).count();
    // tot_leave_dur+=dur_leave;
    delete tran;
    // std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    // int64_t dur = std::chrono::duration_cast<std::chrono::milliseconds> (end - begin).count();
    // tot_end_dur+=dur;
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
    //std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    Region* reg = (Region*) shared;
    Transaction* tran = reinterpret_cast<Transaction*>(tx);
    for (size_t i = 0; i < size; i+=reg->align) {
        void* word = const_cast<void*>(source) + i;
        void* new_target = target + i;
        //pair<void*, WordControl*> word_struct;
        shared_lock<shared_mutex> lock_m_shared{reg->lock_mem};
        pair<void*, WordControl*> word_struct = reg->memory.at(word);
        lock_m_shared.unlock();
        void* read_copy = word_struct.first + (word_struct.second->read_version ? reg->align : 0);
        if (likely(tran->is_ro)) {
            memcpy(new_target, read_copy, reg->align);
            continue;
        }
        void* write_copy = word_struct.first + (word_struct.second->read_version ? 0 : reg->align);
        // if (likely(word_struct.second->written.load())) {
        //     if (likely(word_struct.second->access.load() == tran->t_id)) {
        //         memcpy(new_target, write_copy, reg->align);
        //         continue;
        //     } else {
        //         reg->batcher->leave();
        //         tran->failed = true;
        //         delete tran;
        //         return false;
        //     }
        // } else {
        //     word_struct.second->access.store(tran->t_id);
        //     memcpy(new_target, read_copy, reg->align);
        //     continue;
        // }
        // if (likely(word_struct.second->write_tran.load() == tran->t_id)) {
        //     word_struct.second->read_tran.store(tran->t_id);
        //     memcpy(new_target, write_copy, reg->align);
        //     continue;
        // } else if (likely(word_struct.second->write_tran.load() == -1)) {
        //     // TODO: check if we need a lock shared here just before the copy 
        //     // (avoiding someone to start writing on the word after we checked the -1)
        //     memcpy(new_target, read_copy, reg->align);
        //     word_struct.second->read_tran.store(tran->t_id);
        // } else {
        //     tran->failed = true;
        //     delete tran;
        //     reg->batcher->leave();
        //     return false;
        // }
        if (likely(word_struct.second->access.load() == tran->t_id)) {
            memcpy(new_target, write_copy, reg->align);
            continue;
        } else if (word_struct.second->access.load() == -1) {
            memcpy(new_target, read_copy, reg->align);
            continue;
        } else {
            tran->failed = true;
            if (unlikely(tran->first_allocs.size()!=0)) {
                unique_lock<shared_mutex> lock_m{reg->lock_mem};
                for (auto const& word: tran->allocated) {
                    delete reg->memory.at(word).second;
                    reg->memory.erase(word);
                }
                lock_m.unlock();
                reg->memory_sizes.erase(word);
            }
            delete tran;
            reg->batcher->leave();
            // std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
            // int64_t dur = std::chrono::duration_cast<std::chrono::milliseconds> (end - begin).count();
            // tot_read_dur+=dur;
            // tot_abort_read++;
            return false;
        }
    }
    // std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    // int64_t dur = std::chrono::duration_cast<std::chrono::milliseconds> (end - begin).count();
    // tot_read_dur+=dur;
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
    //std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    Region* reg = (Region*) shared;
    Transaction* tran = reinterpret_cast<Transaction*>(tx);
    for (size_t i = 0; i < size; i+=reg->align) {
        void* word = target + i;
        void* new_source = const_cast<void*>(source) + i;
        shared_lock<shared_mutex> lock_m_shared{reg->lock_mem};
        pair<void*, WordControl*> word_struct = reg->memory.at(word);
        lock_m_shared.unlock();
        void* write_copy = word_struct.first + (word_struct.second->read_version ? 0 : reg->align);
        // int expected_tran = -1;
        // bool expected_written = false;
        // if (unlikely(word_struct.second->written.load())) {
        //     if (unlikely(word_struct.second->access.load() == tran->t_id)){
        //         memcpy(write_copy, new_source, reg->align);
        //         tran->writes.push_back(word_struct.second);
        //         continue;
        //     } else {
        //         reg->batcher->leave();
        //         tran->failed = true;
        //         delete tran;
        //         return false;
        //     }
        // } else {
        //     if (unlikely(word_struct.second->access.load() != tran->t_id && word_struct.second->access.load() != -1)){
        //         reg->batcher->leave();
        //         tran->failed = true;
        //         delete tran;
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
        //     } else if (likely(expected_tran == tran->t_id)) {
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
        // int expected = -1;
        // if (likely(word_struct.second->write_tran.compare_exchange_weak(expected, tran->t_id))) {
        //     if (unlikely(word_struct.second->read_tran.compare_exchange_weak(expected, tran->t_id))) {
        //         word_struct.second->write_tran.store(tran->t_id);
        //         memcpy(write_copy, new_source, reg->align);
        //         tran->writes.push_back(word_struct.second);
        //         continue;
        //     } else if (unlikely(expected == tran->t_id)) {
        //         word_struct.second->write_tran.store(tran->t_id);
        //         memcpy(write_copy, new_source, reg->align);
        //         tran->writes.push_back(word_struct.second);
        //         continue;
        //     }
        //     else {
        //         //removeT(tran, true);
        //         tran->failed = true;
        //         delete tran;
        //         reg->batcher->leave();
        //         return false;
        //     }
        //     // // May be not needed, a read would check just the write and another write would abort anyway at the write check
        //     // // word_struct.second->read_tran.store(tran->t_id);
        //     // memcpy(write_copy, target+i, reg->align);
        //     // continue;
        // } else if (unlikely(expected == tran->t_id)) {
        //     memcpy(write_copy, new_source, reg->align);
        //     continue;
        // } else {
        //     //removeT(tran, true);
        //     tran->failed = true;
        //     delete tran;
        //     reg->batcher->leave();
        //     return false;
        // }
        int expected = -1;
        if (likely(word_struct.second->access.compare_exchange_strong(expected, tran->t_id))) {
            memcpy(write_copy, new_source, reg->align);
            tran->writes.insert(word_struct.second);
            //tran->writes.push_back(word_struct.second);
            continue;
        } else if (likely(expected == tran->t_id)) {
            memcpy(write_copy, new_source, reg->align);
            continue;
        } else {
            //removeT(tran, true);
            tran->failed = true;
            if (unlikely(tran->first_allocs.size()!=0)) {
                unique_lock<shared_mutex> lock_m{reg->lock_mem};
                for (auto const& word: tran->allocated) {
                    delete reg->memory.at(word).second;
                    reg->memory.erase(word);
                }
                lock_m.unlock();
                reg->memory_sizes.erase(word);
            }
            delete tran;
            reg->batcher->leave();
            // std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
            // int64_t dur = std::chrono::duration_cast<std::chrono::milliseconds> (end - begin).count();
            // tot_write_dur+=dur;
            // tot_abort_write++;
            return false;
        }
    }
    // std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    // int64_t dur = std::chrono::duration_cast<std::chrono::milliseconds> (end - begin).count();
    // tot_write_dur+=dur;
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
    void* new_seg = calloc(size*2/reg->align, reg->align);
    if (unlikely(new_seg == nullptr)) {
        return Alloc::nomem;
    }
    void* iter_word = new_seg;
    tran->first_allocs.push_back(new_seg);
    unique_lock<shared_mutex> lock_m{reg->lock_mem};
    for (size_t i = 0; i < size; i+=reg->align) {
        tran->allocated.push_back(new_seg+i);
        reg->memory[new_seg+i] = make_pair(iter_word, new WordControl());
        iter_word = iter_word + 2*reg->align;
    }
    lock_m.unlock();
    reg->memory_sizes[new_seg] = size;
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