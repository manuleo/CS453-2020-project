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

inline void* get_word (Region* reg, void* word) {
    ptrdiff_t id = ((char*)word - (char*)reg->start)/reg->align;
    return reg->start + id*sizeof(Word);
}

inline void clear_writes(Region* reg, vector<void*>* writes) {
    for (auto const& write: *writes) {
        atomic_uint* access = (atomic_uint*) (write + 2*reg->align);
        access->store(0);
    }
    writes->clear();
}

// -------------------------------------------------------------------------- //

// atomic_int64_t tot_read_dur;
// atomic_int64_t tot_write_dur;
// atomic_int64_t tot_leave_dur;
// atomic_int64_t tot_enter_dur;
// atomic_int64_t tot_end_dur;
// atomic_int64_t tot_alloc_dur;
// atomic_uint tot_abort_read;
// atomic_uint tot_abort_write;
// atomic_uint tot_read;
// atomic_uint tot_write;
// atomic_uint tot_enter;
// atomic_uint tot_leave;
// atomic_uint tot_end;
// atomic_uint tot_alloc;

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
    // TODO: may change to 4GB in production
    reg->start = mmap(NULL, MAX_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (unlikely(reg->start == nullptr)) {
        delete reg;
        return invalid_shared;
    }
    reg->next_segment = reg->start + size*sizeof(Word);
    reg->tot_size+=size;
    return reg;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) noexcept {
    Region* reg = (Region*) shared;
    munmap(reg->start, MAX_SIZE);
    if (unlikely(reg->written.size!=0))
        reg->written.destroy();
    delete reg->batcher;
    delete reg;

    // cout << "TOT end: " << (float) tot_end_dur << endl;
    // cout << "TOT read: " << (float) tot_read_dur << endl;
    // cout << "TOT write: " << (float) tot_write_dur << endl;
    // cout << "TOT enter: " << (float) tot_enter_dur << endl;
    // cout << "TOT leave: " << (float) tot_leave_dur << endl;
    // cout << "TOT alloc: " << (float) tot_alloc_dur << endl;
    // cout << "TOT end batch: " << (float) reg->end_epoch_dur << endl;
    // cout << "AVG end: " << (float) tot_end_dur / (float) tot_end << endl;
    // cout << "AVG read: " << (float) tot_read_dur / (float) tot_read << endl;
    // cout << "AVG write: " << (float) tot_write_dur / (float) tot_write << endl;
    // cout << "AVG enter: " << (float) tot_enter_dur / (float) tot_enter << endl;
    // cout << "AVG leave: " << (float) tot_leave_dur / (float) tot_leave << endl;
    // cout << "AVG alloc: " << (float) tot_alloc_dur / (float) tot_alloc << endl;
    // cout << "AVG end batch: " << (float) reg->end_epoch_dur / (float) reg->count_end << endl;
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
    return ((Region*)shared)->start;
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
    // std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    reg->batcher->enter(is_ro);
    // std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    // int64_t dur = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
    // tot_enter_dur+=dur;
    // tot_enter++;
    return reinterpret_cast<tx_t>(tran);
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t tx) noexcept {
    // std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    Region* reg = (Region*) shared;
    Transaction* tran = reinterpret_cast<Transaction*>(tx);
    if (unlikely(!tran->is_ro)) {
        if (likely(tran->writes.size()!=0)) {
            for (auto const& write: tran->writes)
                reg->written.add(write);
        }
    }
    // std::chrono::steady_clock::time_point begin_leave = std::chrono::steady_clock::now();
    reg->batcher->leave(false);
    // std::chrono::steady_clock::time_point end_leave = std::chrono::steady_clock::now();
    // int64_t dur_leave = std::chrono::duration_cast<std::chrono::nanoseconds> (end_leave - begin_leave).count();
    // tot_leave_dur+=dur_leave;
    // tot_leave++;
    delete tran;
    // std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    // int64_t dur = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
    // tot_end_dur+=dur;
    // tot_end++;
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
    // std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    // tot_read++;
    Region* reg = (Region*) shared;
    Transaction* tran = reinterpret_cast<Transaction*>(tx);
    for (size_t i = 0; i < size; i+=reg->align) {
        void* user_word = const_cast<void*>(source) + i;
        void* new_target = target + i;
        void* word = get_word(reg, user_word);
        atomic_uint* access = (atomic_uint*) (word + 2*reg->align);
        bool* read_version = (bool*)(word + 2*reg->align + sizeof(atomic_uint) + 1);
        void* read_copy = word + (*read_version ? reg->align : 0);
        if (likely(tran->is_ro)) {
            memcpy(new_target, read_copy, reg->align);
            continue;
        }
        void* write_copy = word + (*read_version ? 0 : reg->align);
        if (likely(access->load() == tran->t_id)) {
            memcpy(new_target, write_copy, reg->align);
            continue;
        } else if (likely(access->load() == 0)) {
            memcpy(new_target, read_copy, reg->align);
            continue;
        } else {
            clear_writes(reg, &tran->writes);
            delete tran;
            // std::chrono::steady_clock::time_point begin_leave = std::chrono::steady_clock::now();
            reg->batcher->leave(true);
            // std::chrono::steady_clock::time_point end_leave = std::chrono::steady_clock::now();
            // int64_t dur_leave = std::chrono::duration_cast<std::chrono::nanoseconds> (end_leave - begin_leave).count();
            // tot_leave_dur+=dur_leave;
            // tot_leave++;
            // std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
            // int64_t dur = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
            // tot_read_dur+=dur;
            // tot_abort_read++;
            return false;
        }
    }
    // std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    // int64_t dur = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
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
    // std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    // tot_write++;
    Region* reg = (Region*) shared;
    Transaction* tran = reinterpret_cast<Transaction*>(tx);
    for (size_t i = 0; i < size; i+=reg->align) {
        void* user_word = target + i;
        void* new_source = const_cast<void*>(source) + i;
        void* word = get_word(reg, user_word);
        atomic_uint* access = (atomic_uint*) (word + 2*reg->align);
        bool* read_version = (bool*)(word + 2*reg->align + sizeof(atomic_uint) + 1);
        void* write_copy = word + (*read_version ? 0 : reg->align);
        uint expected = 0;
        if (likely(access->compare_exchange_weak(expected, tran->t_id))) {
            memcpy(write_copy, new_source, reg->align);
            tran->writes.push_back(word);
            continue;
        } else if (likely(expected == tran->t_id)) {
            memcpy(write_copy, new_source, reg->align);
            continue;
        } else {
            //removeT(tran, true);
            clear_writes(reg, &tran->writes);
            delete tran;
            // std::chrono::steady_clock::time_point begin_leave = std::chrono::steady_clock::now();
            reg->batcher->leave(true);
            // std::chrono::steady_clock::time_point end_leave = std::chrono::steady_clock::now();
            // int64_t dur_leave = std::chrono::duration_cast<std::chrono::nanoseconds> (end_leave - begin_leave).count();
            // tot_leave_dur+=dur_leave;
            // tot_leave++;
            // std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
            // int64_t dur = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
            // tot_write_dur+=dur;
            // tot_abort_write++;
            return false;
        }
    }
    // std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    // int64_t dur = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
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
    // tot_alloc++;
    // std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    Region* reg = (Region*) shared;
    Transaction* tran = reinterpret_cast<Transaction*>(tx);
    lock_guard<mutex> lock_all{reg->lock_alloc};
    if (unlikely(reg->tot_size + size > MAX_SIZE)) {
        return Alloc::nomem;
    }
    *target = reg->next_segment;
    reg->next_segment = reg->next_segment + size*sizeof(Word);
    reg->tot_size+=size;
    // std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    // int64_t dur = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();
    // tot_alloc_dur+=dur;
    //cout << "Successfully allocated a new segment! (Still need to be committed)" << endl;
    return Alloc::success;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared as (unused), tx_t tx as (unused), void* target as (unused)) noexcept {
    return true;
}