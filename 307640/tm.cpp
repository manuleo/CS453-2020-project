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
    shared_ptr<MemoryObject> first = make_shared<MemoryObject>();
    if (unlikely(!first)) {
        free(reg);
        return invalid_shared;
    }
    VersionTuple* first_tuple = new VersionTuple(0, NULL);
    if (unlikely(!first_tuple)) {
        free(reg);
        return invalid_shared;
    }
    if (unlikely(posix_memalign(&(first_tuple->data), align, size) != 0)) {
        free(reg);
        free(first_tuple);
        return invalid_shared;
    }
    memset(first_tuple->data, 0, size);
    first->versions.push_back(first_tuple);
    reg->memory.push_back(first);
    return reg;
}

void removeT(Region* reg, tx_t tx) {
    lock_guard<recursive_mutex> lock_trans(reg->lock_trans);
    shared_ptr<TransactionObject> tran = reg->trans.at(tx-1);
    for(shared_ptr<Write> write: tran->writes) {
        if(write->data!=NULL)
            free(write->data);
    }
    reg->trans.erase(reg->trans.begin()+tx-1);
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) noexcept {
    Region* reg = (Region*) shared;
    for(shared_ptr<MemoryObject> mem: reg->memory) {
        for(VersionTuple* ver: mem->versions) {
            free(ver->data);
            free(ver);
        }
    }
    for(shared_ptr<TransactionObject>tran: reg->trans) {
        removeT(reg, tran->t_id);
    }
    free(reg);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) noexcept {
    return ((Region*)shared)->memory.front().get();
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
    const lock_guard<recursive_mutex> lock(reg->lock_trans);
    int t_id = reg->t_count + 1;
    reg->t_count = reg-> t_count + 1;
    shared_ptr<TransactionObject> tran = make_shared<TransactionObject>(t_id, is_ro);
    if (unlikely(!tran)) {
        return invalid_tx;
    }
    reg->trans.push_back(tran);
    return tran->t_id;
}

bool check_version(int t_id, shared_ptr<MemoryObject> obj) noexcept {
    for(VersionTuple* version: obj->versions) {
        for(int read_id: version->readList) {
            if(version->ts < t_id && t_id < read_id)
                return false;
        }
    }
    return true;
}

bool check_free(int t_id, shared_ptr<MemoryObject> obj) noexcept {
    for(VersionTuple* version: obj->versions) {
        if (version->ts > t_id)
            return false;
        for(int read_id: version->readList) {
            if(t_id < read_id)
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
    unique_lock<recursive_mutex> lock_trans(reg->lock_trans);
    shared_ptr<TransactionObject> tran = reg->trans.at(tx-1);
    lock_trans.unlock();
    vector<shared_ptr<lock_guard<recursive_mutex>>> my_locks;
    for (shared_ptr<MemoryObject> mem: tran->reads) {
        my_locks.push_back(make_shared<lock_guard<recursive_mutex>>(mem->lock));
        if(mem->id_deleted!=-1 && mem->id_deleted < tran->t_id) {
            removeT(reg, tx);
            return false;
        }
    }
    if(tran->is_ro == true) {
        removeT(reg, tx);
        return true;
    }
    for(shared_ptr<Write> write: tran->writes) {
        my_locks.push_back(make_shared<lock_guard<recursive_mutex>>(write->object->lock));
        if(!check_version(tran->t_id, write->object)) {
            removeT(reg, tx);
            return false;
        }
        if (write->type == WriteType::del) {
            if(!check_free(tran->t_id, write->object)) {
            removeT(reg, tx);
            return false;
            }
        }
    }

    for (shared_ptr<Write> write: tran->writes) {
        if(write->type == WriteType::del) 
            write->object->id_deleted = tran->t_id;
        else if (write->type == WriteType::write) {
            int i = 0;
            VersionTuple* new_version = new VersionTuple(tran->t_id, NULL);
            if (unlikely(posix_memalign(&(new_version->data), reg->align, write->size) != 0)) {
                free(new_version);
                removeT(reg, tx); //TODO: what happens if I abort here??
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
        else if (write->type == WriteType::write) {
            VersionTuple* new_version = new VersionTuple(tran->t_id, NULL);
            if (unlikely(posix_memalign(&(new_version->data), reg->align, write->size) != 0)) {
                free(new_version);
                removeT(reg, tx); //TODO: what happens if I abort here??
            }
            memcpy(new_version->data, write->data, write->size);
            write->object->versions.push_back(new_version);
            unique_lock<recursive_mutex> lock_m(reg->lock_mem);
            reg->memory.push_back(write->object);
            lock_m.unlock();
        }
    }
    removeT(reg, tx);
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
    // May be not needed to lock here
    unique_lock<recursive_mutex> lock(reg->lock_trans);
    shared_ptr<TransactionObject> tran = reg->trans.at(tx-1);
    lock.unlock();
    unique_lock<recursive_mutex> lock_obj(obj->lock);
    // TODO: may check directly here the id of whom as freed the segment and abort
    for(auto& write: tran->writes) {
        if(write->object.get() == obj) {
            memcpy(target, write->data, size);
            write->read = true;
            return true;
        }
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
        removeT(reg, tx);
        return false;
    }
    best_vers->readList.push_back(t_id);
    lock_obj.unlock();
    memcpy(target, best_vers->data, size);
    tran->reads.push_back(shared_ptr<MemoryObject>(obj));
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
    // May be not needed to lock here
    unique_lock<recursive_mutex> lock(reg->lock_trans);
    shared_ptr<TransactionObject> tran = reg->trans.at(tx-1);
    lock.unlock();
    bool found = false;
    for(auto& write: tran->writes) {
        if(write->object.get() == obj) {
            memcpy(write->data, source, size);
            found = true;
        }
    }
    if (!found) {
        shared_ptr<Write> new_write = make_shared<Write>(shared_ptr<MemoryObject>(obj), size, WriteType::write);
        if(unlikely(!new_write)) {
            removeT(reg, tx);
            return false;
        }
        if (unlikely(posix_memalign(&(new_write->data), reg->align, size) != 0)) {
            removeT(reg, tx);
            return false;
        }
        memcpy(new_write->data, source, size);
        tran->writes.push_back(new_write);
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
    // May be not needed to lock here
    unique_lock<recursive_mutex> lock(reg->lock_trans);
    shared_ptr<TransactionObject> tran = reg->trans.at(tx-1);
    lock.unlock();
    shared_ptr<MemoryObject> mem = make_shared<MemoryObject>();
    if (unlikely(!mem)) {
        return Alloc::nomem;
    }
    lock_guard<recursive_mutex> lock_obj(mem->lock);
    VersionTuple* first_tuple = new VersionTuple(tran->t_id, NULL);
    if (unlikely(!first_tuple)) {
        return Alloc::nomem;
    }
    if (unlikely(posix_memalign(&(first_tuple->data), reg->align, size) != 0)) {
        free(first_tuple);
        return Alloc::nomem;
    }
    memset(first_tuple->data, 0, size);
    mem->versions.push_back(first_tuple);
    *target = mem.get();
    shared_ptr<Write> new_write = make_shared<Write>(mem, 0, WriteType::alloc);
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
    unique_lock<recursive_mutex> lock(reg->lock_trans);
    shared_ptr<TransactionObject> tran = reg->trans.at(tx-1);
    lock.unlock();
    shared_ptr<Write> new_write = make_shared<Write>(shared_ptr<MemoryObject>(obj), 0, WriteType::del);
    tran->writes.push_back(new_write);
    return true;
}
