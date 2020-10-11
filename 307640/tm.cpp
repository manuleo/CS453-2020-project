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
    if (unlikely(!(reg->lock_mem.init()))) {
        free(reg);
        return invalid_shared;
    }
    if (unlikely(!(reg->lock_trans.init()))) {
        free(reg);
        return invalid_shared;
    }
    MemoryObject* first = new MemoryObject(true);
    if (unlikely(!first)) {
        free(reg);
        return invalid_shared;
    }
    if (unlikely(!(first->lock.init()))) {
        free(reg);
        free(first);
        return invalid_shared;
    }
    VersionTuple* first_tuple = new VersionTuple(0, NULL);
    if (unlikely(!first_tuple)) {
        free(reg);
        free(first);
        return invalid_shared;
    }
    if (unlikely(posix_memalign(&(first_tuple->data), align, size) != 0)) {
        free(reg);
        free(first);
        free(first_tuple);
        return invalid_shared;
    }
    memset(first_tuple->data, 0, size);
    first->versions.push_back(first_tuple);
    reg->memory.push_back(first);
    return reg;
}

void removeT(Region* reg, tx_t tx, bool abort) {
    if (unlikely(!(reg->lock_trans.lock()))) {
        throw exception();
    }
    TransactionObject* tran = reg->trans.at(tx-1);
    if(abort) {
        vector<Write*> allocations;
        copy_if(tran->writes.begin(), tran->writes.end(), back_inserter(allocations), [](Write* w){return w->type==WriteType::alloc;} );
        for(Write* writealloc: allocations) {
            if(unlikely(!(writealloc->object->lock.lock()))) { //TODO: check on this: 2 subsequent acquire on the same lock from same thread???
                throw exception();
            }
            writealloc->object->is_valid = false; // Mark allocated object as not valid anymore
            writealloc->object->lock.unlock();
        }
    }
    for(Write* write: tran->writes) {
        if(write->data!=NULL)
            free(write->data);
        free(write);
    }
    reg->trans.erase(reg->trans.begin()+tx-1);
    free(tran);
    reg->lock_trans.unlock();
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) noexcept {
    Region* reg = (Region*) shared;
    for(MemoryObject* mem: reg->memory) {
        for(VersionTuple* ver: mem->versions) {
            free(ver->data);
            free(ver);
        }
        free(mem);
    }
    for(TransactionObject* tran: reg->trans) {
        removeT(reg, tran->t_id, false);
    }
    free(reg);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) noexcept {
    return ((Region*)shared)->memory.front();
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
    if (unlikely(!reg->lock_trans.lock()))
            return invalid_tx;
    int t_id = reg->t_count + 1;
    reg->t_count = reg-> t_count + 1;
    TransactionObject* tran = new TransactionObject(t_id, is_ro);
    if (unlikely(!tran)) {
        return invalid_tx;
    }
    reg->trans.push_back(tran);
    reg->lock_trans.unlock();
    return tran->t_id;
}

bool check_version(int t_id, MemoryObject* obj) noexcept {
    if(!obj->is_valid) {
        return false;
    }
    for(VersionTuple* version: obj->versions) {
        for(int read_id: version->readList) {
            if(version->ts < t_id && t_id < read_id)
                return false;
        }
    }
    return true;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t tx) noexcept {
    Region* reg = (Region*) shared;
    if (unlikely(!reg->lock_trans.lock())) {
        removeT(reg, tx, true);
        return false;
    }
    TransactionObject* tran = reg->trans.at(tx-1);
    reg->lock_trans.unlock();
    if(tran->is_ro == true) {
        removeT(reg, tx, false);
        return true;
    }
    vector<Lock> lock_list;
    for(Write* write: tran->writes) {
        if(unlikely(!(write->object->lock.lock()))) { //TODO: check on this: 2 subsequent acquire on the same lock from same thread??? Using mutex recursive could solve it
            removeT(reg, tx, true);
            return false;
        }
        lock_list.push_back(write->object->lock);
        if(!check_version(tran->t_id, write->object)) {
            for(Lock lock: lock_list)
                lock.unlock();
            removeT(reg, tx, true);
            return false;
        }

    }
    vector<Write*> writes;
    copy_if(tran->writes.begin(), tran->writes.end(), back_inserter(writes), [](Write* w){return w->type==WriteType::write;} );

    for (Write* write: writes) {
        int i = 0;
        VersionTuple* new_version = new VersionTuple(tran->t_id, NULL);
        if (unlikely(posix_memalign(&(new_version->data), reg->align, write->size) != 0)) {
            free(new_version);
            removeT(reg, tx, true); //TODO: what happens if I abort here??
            return false;
        }
        memcpy(new_version->data, write->data, write->size);
        bool inserted = false;
        for(VersionTuple* version: write->object->versions) {
            if(version->ts > tran->t_id) {
                write->object->versions.insert(write->object->versions.begin() + i, new_version);
                inserted = true;
                break;
            }
            i+=1;
        }
        if(!inserted)
            write->object->versions.push_back(new_version);

    }
    vector<Write*> frees;
    copy_if(tran->writes.begin(), tran->writes.end(), back_inserter(frees), [](Write* w){return w->type==WriteType::del;} );
    
    for (Write* write: frees) {
        if (unlikely(!reg->lock_mem.lock())) {
            removeT(reg, tx, true);
            return false;
        }
        int i=0;
        for (MemoryObject* mem: reg->memory) {
            if(mem==write->object){
                reg->memory.erase(reg->memory.begin()+i);
                Lock lock = mem->lock;
                for (VersionTuple* ver: mem->versions) { //TODO: Maybe I should remove only the version of this transaction?
                    free(ver->data);
                    free(ver);
                }
                free(mem);
                lock.unlock();
            }
            i+=1;
        }
    }
    for(Lock lock: lock_list)
        lock.unlock();
    reg->lock_mem.unlock();
    removeT(reg, tx, false);
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
    MemoryObject* obj = (MemoryObject*) source;
    if (unlikely(!reg->lock_trans.lock())) {
        removeT(reg, tx, true);
        return false;
    } 
    TransactionObject* tran = reg->trans.at(tx-1);
    reg->lock_trans.unlock();
    if (unlikely(!(obj->lock.lock()))) {
        removeT(reg, tx, true);
        return false;
    }
    if(!obj->is_valid) {
        removeT(reg, tx, true);
        return false;
    }
    int t_id = tran->t_id;
    VersionTuple* best_vers = nullptr;
    int best_ts = 0;
    for(VersionTuple* ver: obj->versions) {
        if((ver->ts < t_id) && (best_ts < ver->ts)) {
            best_vers = ver;
            best_ts = ver->ts;
        }
    }
    if (best_vers==nullptr) {
        removeT(reg, tx, true);
        return false;
    }
    best_vers->readList.push_back(t_id);
    memcpy(target, best_vers->data, size);
    obj->lock.unlock();
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
    MemoryObject* obj = (MemoryObject*) target;
    if (unlikely(!reg->lock_trans.lock())) {
        removeT(reg, tx, true);
        return false;
    }
    TransactionObject* tran = reg->trans.at(tx-1);
    reg->lock_trans.unlock();
    if (unlikely(!obj->lock.lock())) {
        removeT(reg, tx, true);
        return false;
    }
    if(!obj->is_valid) {
        removeT(reg, tx, true);
        return false;
    }
    obj->lock.unlock();
    Write* new_write = new Write(obj, size, WriteType::write);
    if(unlikely(!new_write)) {
        removeT(reg, tx, true);
        return false;
    }
    if (unlikely(posix_memalign(&(new_write->data), reg->align, size) != 0)) {
        free(new_write);
        removeT(reg, tx, true);
        return false;
    }
    memcpy(new_write->data, source, size);
    tran->writes.push_back(new_write);
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
    if (unlikely(!reg->lock_trans.lock()))
            return Alloc::abort;
    TransactionObject* tran = reg->trans.at(tx-1);
    reg->lock_trans.unlock();
    MemoryObject* mem = new MemoryObject(true);
    if (unlikely(!mem)) {
        return Alloc::nomem;
    }
    if (unlikely(!(mem->lock.init()))) {
        free(mem);
        removeT(reg, tx, true);
        return Alloc::abort;
    }
    if (unlikely(!(mem->lock.lock())))
            return Alloc::abort;
    VersionTuple* first_tuple = new VersionTuple(tran->t_id, NULL);
    if (unlikely(!first_tuple)) {
        free(mem);
        return Alloc::nomem;
    }
    if (unlikely(posix_memalign(&(first_tuple->data), reg->align, size) != 0)) {
        free(mem);
        free(first_tuple);
        return Alloc::nomem;
    }
    memset(first_tuple->data, 0, size);
    mem->versions.push_back(first_tuple);
    *target = mem;
    if (unlikely(!(reg->lock_mem.lock()))) {
        free(mem);
        free(first_tuple);
        removeT(reg, tx, true);
        return Alloc::abort;
    }
    reg->lock_mem.unlock();
    mem->lock.unlock();
    Write* new_write = new Write(mem, 0, WriteType::alloc);
    tran->writes.push_back(new_write);
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
    MemoryObject* obj = (MemoryObject*) target;
    if (unlikely(!obj->lock.lock())) {
        removeT(reg, tx, true);
        return false;
    }
    if(!obj->is_valid) {
        removeT(reg, tx, true);
        return false;
    }
    obj->lock.unlock();
    if (unlikely(!reg->lock_trans.lock())){
        removeT(reg, tx, true);
        return false;
    }
    TransactionObject* tran = reg->trans.at(tx-1);
    reg->lock_trans.unlock();
    Write* new_write = new Write(obj, 0, WriteType::del);
    tran->writes.push_back(new_write);
    return true;
}
