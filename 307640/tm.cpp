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
    reg->batcher->enter(is_ro);
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
        if (likely(tran->writes.size()!=0)) {
            for (auto const& write: tran->writes)
                reg->written.add(write);
        }
    }
    reg->batcher->leave(false);
    delete tran;
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
        void* user_word = const_cast<void*>(source) + i;
        void* new_target = target + i;
        void* word = get_word(reg, user_word);
        atomic_uint* access = (atomic_uint*) (word + 2*reg->align);
        bool* read_version = (bool*)(word + 2*reg->align + sizeof(atomic_uint) + 3);
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
            reg->batcher->leave(true);
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
        void* user_word = target + i;
        void* new_source = const_cast<void*>(source) + i;
        void* word = get_word(reg, user_word);
        atomic_uint* access = (atomic_uint*) (word + 2*reg->align);
        bool* read_version = (bool*)(word + 2*reg->align + sizeof(atomic_uint) + 3);
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
            clear_writes(reg, &tran->writes);
            delete tran;
            reg->batcher->leave(true);
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
    lock_guard<mutex> lock_all{reg->lock_alloc};
    if (unlikely(reg->tot_size + size > MAX_SIZE)) {
        return Alloc::nomem;
    }
    *target = reg->next_segment;
    reg->next_segment = reg->next_segment + size*sizeof(Word);
    reg->tot_size+=size;
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