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

// Requested features
#define _GNU_SOURCE
#define _POSIX_C_SOURCE   200809L
#ifdef __STDC_NO_ATOMICS__
    #error Current C11 compiler does not support atomic operations
#endif

// External headers
#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#if (defined(__i386__) || defined(__x86_64__)) && defined(USE_MM_PAUSE)
    #include <xmmintrin.h>
#else
    #include <sched.h>
#endif

// Internal headers
#include <tm.h>

// -------------------------------------------------------------------------- //

/** Define a proposition as likely true.
 * @param prop Proposition
**/
#undef likely
#ifdef __GNUC__
    #define likely(prop) \
        __builtin_expect((prop) ? 1 : 0, 1)
#else
    #define likely(prop) \
        (prop)
#endif

/** Define a proposition as likely false.
 * @param prop Proposition
**/
#undef unlikely
#ifdef __GNUC__
    #define unlikely(prop) \
        __builtin_expect((prop) ? 1 : 0, 0)
#else
    #define unlikely(prop) \
        (prop)
#endif

/** Define one or several attributes.
 * @param type... Attribute names
**/
#undef as
#ifdef __GNUC__
    #define as(type...) \
        __attribute__((type))
#else
    #define as(type...)
    #warning This compiler has no support for GCC attributes
#endif

/** Pause for a very short amount of time.
**/
static inline void pause() {
#if (defined(__i386__) || defined(__x86_64__)) && defined(USE_MM_PAUSE)
    _mm_pause();
#else
    sched_yield();
#endif
}

// Defining lock using test-and-set
struct lock_t {
    atomic_bool locked; // Whether the lock is taken
};

/** Initialize the given lock.
 * @param lock Lock to initialize
 * @return Whether the operation is a success
**/
static bool lock_init(struct lock_t* lock) {
    atomic_init(&(lock->locked), false);
    return true;
}

// /** Clean the given lock up.
//  * @param lock Lock to clean up
// **/
// static void lock_cleanup(struct lock_t* lock as(unused)) {
//     return;
// }

/** Wait and acquire the given lock.
 * @param lock Lock to acquire
 * @return Whether the operation is a success
**/
static bool lock_acquire(struct lock_t* lock) {
    bool expected = false;
    while (unlikely(!atomic_compare_exchange_weak_explicit(&(lock->locked), &expected, true, memory_order_acquire, memory_order_relaxed))) {
        expected = false;
        while (unlikely(atomic_load_explicit(&(lock->locked), memory_order_relaxed)))
            pause();
    }
    return true;
}

/** Release the given lock.
 * @param lock Lock to release
**/
static void lock_release(struct lock_t* lock) {
    atomic_store_explicit(&(lock->locked), false, memory_order_release);
}

// Data structures

typedef struct read_node {
    int readId;
    struct read_node* next;
    struct read_node* prev;
} readNode;

typedef struct version_tuple {
    int ts;
    void* data;
    readNode* readList;
    struct version_tuple* next;
    struct version_tuple* prev;
} versionTuple;

typedef struct mem {
    struct lock_t lock;
    versionTuple* versions;
    struct mem* next;
    struct mem* prev;
} memory;

typedef struct write_set {
    memory* object;
    void* data;
    size_t size;
    struct write_set* next;
    struct write_set* prev;
} writeSet;

typedef struct trans_obj {
    int tId;
    bool is_ro;
    writeSet* write;
    struct trans_obj* next;
    struct trans_obj* prev;
} transactionObj;

typedef struct lock_list {
    struct lock_t lock;
    struct lock_list* next;
    struct lock_list* prev;
} locks;

typedef struct {
    int tCount;
    struct lock_t lockList;
    struct lock_t lockMem;
    memory* mem;
    transactionObj* trans;
    size_t size;
    size_t align;
} region;

// Utility functions for structures

/** Link reset.
 * @param pointer Link to reset
**/
static void link_reset(void* pointer, int type) {
    switch (type)
    {
    case 1:
        {readNode* link = (readNode*) pointer; 
        link->prev = NULL;
        link->next = NULL;
        break;}
    case 2:
        {versionTuple* link = (versionTuple*) pointer; 
        link->prev = NULL;
        link->next = NULL;
        break;}
    case 3:
        {memory* link = (memory*) pointer; 
        link->prev = NULL;
        link->next = NULL;
        break;}
    case 4:
        {writeSet* link = (writeSet*) pointer; 
        link->prev = NULL;
        link->next = NULL;
        break;}
    case 5:
        {transactionObj* link = (transactionObj*) pointer; 
        link->prev = NULL;
        link->next = NULL;
        break;}
    case 6:
        {locks* link = (locks*) pointer; 
        link->prev = NULL;
        link->next = NULL;
        break;}
    }
}

/** Link insertion before a "base" link.
 * @param pointer Link to insert
 * @param base_void Base link relative to which 'link' will be inserted
**/
static void link_insert(void* base_void, void* pointer, int type) {
    switch (type)
    {
    case 1:
        {readNode* link = (readNode*) pointer; 
        readNode* base = (readNode*) base_void; 
        while(base->next!=NULL)
            base = base->next;
        link->next = base->next;
        base->next = link;
        link->prev = base;
        if (link->next!=NULL)
            link->next->prev = link;
        break;}
    case 2:
        {versionTuple* link = (versionTuple*) pointer; 
        versionTuple* base = (versionTuple*) base_void; 
        while(base->next!=NULL)
            base = base->next;
        link->next = base->next;
        base->next = link;
        link->prev = base;
        if (link->next!=NULL)
            link->next->prev = link;
        break;}
    case 3:
        {memory* link = (memory*) pointer; 
        memory* base = (memory*) base_void; 
        while(base->next!=NULL)
            base = base->next;
        link->next = base->next;
        base->next = link;
        link->prev = base;
        if (link->next!=NULL)
            link->next->prev = link;
        break;}
    case 4:
        {writeSet* link = (writeSet*) pointer; 
        writeSet* base = (writeSet*) base_void; 
        while(base->next!=NULL)
            base = base->next;
        link->next = base->next;
        base->next = link;
        link->prev = base;
        if (link->next!=NULL)
            link->next->prev = link;
        break;}
    case 5:
        {transactionObj* link = (transactionObj*) pointer; 
        transactionObj* base = (transactionObj*) base_void; 
        while(base->next!=NULL)
            base = base->next;
        link->next = base->next;
        base->next = link;
        link->prev = base;
        if (link->next!=NULL)
            link->next->prev = link;
        break;}
    case 6:
        {locks* link = (locks*) pointer; 
        locks* base = (locks*) base_void; 
        while(base->next!=NULL)
            base = base->next;
        link->next = base->next;
        base->next = link;
        link->prev = base;
        if (link->next!=NULL)
            link->next->prev = link;
        break;}
    }
}

/** Link removal.
 * @param pointer Link to remove
**/
static void link_remove(void* pointer, int type) {
    switch (type)
    {
    case 1:
        {readNode* prev = ((readNode*) pointer)->prev;
        readNode* next = ((readNode*) pointer)->next;
        prev->next = next;
        next->prev = prev;
        break;}
    case 2:
        {versionTuple* prev = ((versionTuple*) pointer)->prev;
        versionTuple* next = ((versionTuple*) pointer)->next;
        prev->next = next;
        next->prev = prev;
        break;}
    case 3:
        {memory* prev = ((memory*) pointer)->prev;
        memory* next = ((memory*) pointer)->next;
        prev->next = next;
        next->prev = prev;
        break;}
    case 4:
        {writeSet* prev = ((writeSet*) pointer)->prev;
        writeSet* next = ((writeSet*) pointer)->next;
        prev->next = next;
        next->prev = prev;
        break;}
    case 5:
        {transactionObj* prev = ((transactionObj*) pointer)->prev;
        transactionObj* next = ((transactionObj*) pointer)->next;
        prev->next = next;
        next->prev = prev;
        break;}
    case 6:
        {locks* prev = ((locks*) pointer)->prev;
        locks* next = ((locks*) pointer)->next;
        prev->next = next;
        next->prev = prev;
        break;}
    }
}

// Utility functions

void freeM(memory* mem) {
    versionTuple* vers = mem->versions;
    while(true) {
        versionTuple* ver = vers->next;
        readNode* reads = ver->readList;
        if (reads!=NULL) {
            while(true) {
                readNode* read = reads->next;
                if (read==reads)
                    break;
                link_remove(read, 1);
                free(read);
            }
            free(reads);
        }
        if(ver==vers)
            break;
        free(ver->data);
        link_remove(ver, 2);
        free(ver);
    }
    free(vers->data);
    free(vers);
    free(mem);
}

void removeT(region* reg, transactionObj* tran, bool delete_memory) {
    writeSet* writes;
    if (unlikely(!lock_acquire(&(reg->lockList))))
            return;
    transactionObj* head_t = reg->trans, *prev_t = NULL;
    while (head_t!=NULL && head_t==tran) {
        reg->trans = tran -> next;
        writes = head_t->write;
        if (writes!=NULL) {
            while(true) {
                writeSet* write = writes->next;
                if (write==writes)
                    break;
                if (write->data != NULL) {
                    free(write->data);
                    link_remove(write, 4);
                    free(write);
                }
                else if (delete_memory) {
                    memory* mem = write->object;
                    freeM(mem);
                }
            }
            free(writes->data);
            free(writes);
        }
        free(head_t);
        head_t = reg->trans;
        head_t -> prev = NULL;
    }
    while(head_t!=NULL) {
        while(head_t!=NULL && head_t!=tran) {
            prev_t = head_t;
            head_t = head_t->next;
        }
        link_remove(head_t, 5);
        writes = head_t->write;
        if (writes!=NULL) {
            while(true) {
                writeSet* write = writes->next;
                if (write==writes)
                    break;
                if (write->data != NULL) {
                    free(write->data);
                    link_remove(write, 4);
                    free(write);
                }
                else if (delete_memory) {
                    memory* mem = write->object;
                    freeM(mem);
                }
            }
            free(writes->data);
            free(writes);
        }
        free(head_t);
        head_t = prev_t->next;
    }
    lock_release(&(reg->lockList));
    return;
}

bool check_version(int t_id, memory* obj) {
    versionTuple* versions = obj->versions;
    readNode* reads;
    while(versions!=NULL) {
        reads = versions->readList;
        while(reads!=NULL) {
            if (versions->ts < t_id && t_id < reads->readId)
                return false;
            reads = reads->next;
        }
        versions = versions->next;
    }
    return true;
}

// -------------------------------------------------------------------------- //

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align) {
    region* reg = (region*) malloc(sizeof(region));
    if (unlikely(!reg)) {
        return invalid_shared;
    }
    memory* first = (memory*) malloc(sizeof(memory));
    if (unlikely(!first)) {
        free(reg);
        return invalid_shared;
    }
    if (unlikely(!lock_init(&(first->lock)))) {
        free(reg);
        free(first);
        return invalid_shared;
    }
    versionTuple* first_tuple = (versionTuple*) malloc(sizeof(versionTuple));
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
    first_tuple -> ts = 0;
    first_tuple -> readList = NULL;
    link_reset(first_tuple, 2);
    first->versions = first_tuple;
    link_reset(first, 3);
    if (unlikely(!lock_init(&(reg->lockMem)))) {
        free(reg);
        free(first);
        free(first_tuple);
        return invalid_shared;
    }
    if (unlikely(!lock_init(&(reg->lockList)))) {
        free(reg);
        free(first);
        free(first_tuple);
        return invalid_shared;
    }
    reg->trans = NULL;
    reg->align = align;
    reg->size = size;
    reg->tCount = 0;
    reg->mem = first;
    return reg;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) {
    region* reg = (region*) shared;
    transactionObj* live_trans = reg->trans;
    if(live_trans!=NULL) {
        while (true) {
            transactionObj* tran = live_trans->next;
            writeSet* writes = tran->write;
            if (writes!=NULL) {
                while(true) {
                    writeSet* write = writes->next;
                    if (write==writes)
                        break;
                    if (write->data!=NULL)
                        free(write->data);
                    link_remove(write, 4);
                    free(write);
                }
                free(writes->data);
                free(writes);
            }
            if (tran == live_trans)
                break;
            link_remove(tran, 5);
            free(tran);
        }
        free(live_trans);
    }
    memory* memories = reg->mem;
    while(true) {
        memory* m = memories->next;
        versionTuple* vers = m->versions;
        while(true) {
            versionTuple* ver = vers->next;
            readNode* reads = ver->readList;
            if (reads!=NULL) {
                while(true) {
                    readNode* read = reads->next;
                    if (read==reads)
                        break;
                    link_remove(read, 1);
                    free(read);
                }
                free(reads);
            }
            if(ver==vers)
                break;
            free(ver->data);
            link_remove(ver, 2);
            free(ver);
        }
        free(vers->data);
        free(vers);
        if (m == memories)
            break;
        link_remove(m, 3);
        free(m);
    }
    free(memories);
    free(reg);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) {
    return ((region*)shared)->mem;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) {
    return ((region*)shared)->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared) {
    return ((region*)shared)->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared, bool is_ro) {
    region* reg = (region*) shared;
    if (unlikely(!lock_acquire(&(reg->lockList))))
            return invalid_tx;
    int t_id = reg->tCount + 1;
    reg->tCount = reg->tCount + 1;
    transactionObj* tran = (transactionObj*) malloc(sizeof(transactionObj));
    tran->is_ro = is_ro;
    tran->tId = t_id;
    tran->write = NULL;
    link_reset(tran, 4);
    if (reg->trans!=NULL)
        link_insert(tran, reg->trans, 5);
    else
        reg->trans = tran;
    lock_release(&(reg->lockList));
    return tran; // TODO: can't return pointer, need to return handle (uint)
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t tx) {
    region* reg = (region*) shared;
    transactionObj* tran = (transactionObj*) tx;
    writeSet* writes;
    if(tran->is_ro == true) {
        removeT(reg, tran, false);
        return true;
    }
    writes = tran->write;
    int i_write = 0;
    while(writes!=NULL) {
        if (unlikely(!lock_acquire(&(writes->object->lock)))) //TODO: check on this: 2 subsequent acquire on the same lock from same thread???
            return false;
        if(check_version(tran->tId, writes->object)==false) {
            writeSet* writes_unlock = tran->write;
            locks* lock_list = (locks*) malloc(sizeof(locks));
            lock_list->lock = writes_unlock -> object -> lock;
            link_reset(lock_list, 6);
            writes_unlock = writes_unlock->next;
            for (int i = 1; i<=i_write; i++) {
                locks* node_lock = (locks*) malloc(sizeof(locks));
                node_lock -> lock = writes_unlock -> object -> lock;
                link_insert(lock_list, node_lock, 6);
                writes_unlock = writes_unlock->next;
            }
            removeT(reg, tran, true);
            locks* lock_node = lock_list;
            for (int i = 0; i<=i_write; i++) {
                lock_release(&(lock_node->lock));
                lock_node = lock_node -> next;
            }
            lock_node = lock_list;
            while (true) {
                locks* node = lock_node->next;
                if (node == lock_node)
                    break;
                link_remove(node, 6);
                free(node);
            }
            free(lock_node);
            return false;
        }
        writes = writes->next;
        i_write = i_write + 1;
    }
    writes = tran->write;
    while (writes!=NULL) {
        if (writes->data==NULL) {
            writes = writes->next;
            continue;
        }
        memory* mem = writes->object;
        versionTuple* versions = mem->versions;
        versionTuple* new_version = (versionTuple*) malloc(sizeof(versionTuple));
        new_version->data = memcpy(new_version->data, writes->data, writes->size);
        new_version->ts = tran->tId;
        new_version->readList = NULL;
        link_reset(new_version, 6);
        while (versions!=NULL) {
            if (versions->ts > tran->tId) {
                versionTuple* prev = versions->prev;
                prev->next = new_version;
                versions->prev = new_version;
                new_version -> next = versions;
                new_version->prev = prev;
                break;
            }
            else if (versions->next == NULL) {
                versions->next = new_version;
                new_version->prev = versions;
            }
            versions = versions->next;
        }
        writes = writes->next;
    }
    writes = tran->write;
    while (writes!=NULL) {
        lock_release(&(writes->object->lock));
        writes = writes->next;
    }
    removeT(reg, tran, false);
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
bool tm_read(shared_t shared, tx_t tx, void const* source, size_t size, void* target) {
    region* reg = (region*) shared;
    memory* obj = (memory*) source;
    transactionObj* tran = (transactionObj*) tx;
    writeSet* w = tran->write;
    void* my_data = NULL;
    while(w!=NULL) {
        if(w->object==obj)
            my_data = w->data;
        w = w->next;
    }
    if (my_data!=NULL) {
        memcpy(target, my_data, size);
        return true;
    }
    if (unlikely(!lock_acquire(&(obj->lock)))) {
        removeT(reg, tran, true);
        return false;
    }      
    int t_id = tran->tId;
    versionTuple* vers = obj->versions;
    versionTuple* best_vers = NULL;
    int best_ts = 0;
    while(vers!=NULL) {
        if((vers->ts < t_id) && (best_ts < vers->ts)) {
            best_vers = vers;
            best_ts = vers->ts;
        }
    }
    if (best_vers==NULL) {
        removeT(reg, tran, true);
        return false;
    } 
    readNode* read = (readNode*) malloc(sizeof(readNode));
    read->readId = t_id;
    link_reset(read, 1);
    if (best_vers->readList!=NULL)
        link_insert(read, best_vers->readList, 1);
    else
        best_vers->readList = read;
    memcpy(target, best_vers->data, size);
    lock_release(&(obj->lock));
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
bool tm_write(shared_t shared, tx_t tx, void const* source, size_t size, void* target) {
    memory* obj = (memory*) target;
    transactionObj* tran = (transactionObj*) tx;
    region* reg = (region*) shared;
    writeSet* new_write = (writeSet*) malloc(sizeof(writeSet));
    if (unlikely(posix_memalign(&(new_write->data), reg->align, size) != 0)) {
        free(new_write);
        removeT(reg, tran, true);
        return false;
    }
    memcpy(new_write->data, source, size);
    new_write->object = obj;
    new_write->size = size;
    link_reset(new_write, 4);
    if (tran->write!=NULL)
        link_insert(new_write, tran->write, 4);
    else
        tran->write = new_write;
    return true;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
alloc_t tm_alloc(shared_t shared, tx_t tx, size_t size, void** target) {
    transactionObj* tran = (transactionObj*) tx;
    region* reg = (region*) shared;
    memory* mem = (memory*) malloc(sizeof(memory));
    if (unlikely(!mem)) {
        return nomem_alloc;
    }
    if (unlikely(!lock_init(&(mem->lock)))) {
        free(mem);
        removeT(reg, tran, true);
        return abort_alloc;
    }
    if (unlikely(!lock_acquire(&(mem->lock))))
            return abort_alloc;
    versionTuple* first_tuple = (versionTuple*) malloc(sizeof(versionTuple));
    if (unlikely(!first_tuple)) {
        free(mem);
        removeT(reg, tran, true);
        return abort_alloc;
    }
    if (unlikely(posix_memalign(&(first_tuple->data), reg->align, size) != 0)) {
        free(mem);
        free(first_tuple);
        return nomem_alloc;
    }
    memset(first_tuple->data, 0, size);
    first_tuple -> ts = 0;
    first_tuple -> readList = NULL;
    link_reset(first_tuple, 2);
    mem->versions = first_tuple;
    if (unlikely(!lock_acquire(&(reg->lockMem)))) {
        free(mem);
        free(first_tuple);
        removeT(reg, tran, true);
        return abort_alloc;
    }
    link_insert(reg->mem, mem, 3);
    *target = mem;
    lock_release(&(reg->lockMem));
    lock_release(&(mem->lock));
    writeSet* new_write = (writeSet*) malloc(sizeof(writeSet));
    new_write->data=NULL;
    new_write->object=mem;
    link_reset(new_write, 4);
    if (tran->write!=NULL)
        link_insert(new_write, tran->write, 4);
    else
        tran->write = new_write;
    return success_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared, tx_t tx, void* target) {
    memory* obj = (memory*) target;
    transactionObj* tran = (transactionObj*) tx;
    region* reg = (region*) shared;
    writeSet* head_w = tran->write, *prev_w = NULL;
    while (head_w!=NULL && head_w->object==obj) {
        tran->write = head_w -> next;
        free(head_w);
        head_w = tran->write;
    }
    while(head_w!=NULL) {
        while(head_w!=NULL && head_w->object!=obj) {
            prev_w = head_w;
            head_w = head_w->next;
        }
        link_remove(head_w, 4);
        free(head_w->data);
        free(head_w);
        head_w = prev_w->next;
    }
    if (unlikely(!lock_acquire(&(reg->lockMem)))) {
        removeT(reg, tran, true);
        return false;
    }
    versionTuple* vers = obj->versions;
        while(true) {
            versionTuple* ver = vers->next;
            readNode* reads = ver->readList;
            if (reads!=NULL) {
                while(true) {
                    readNode* read = reads->next;
                    if (read==reads)
                        break;
                    link_remove(read, 1);
                    free(read);
                }
                free(reads);
            }
            if(ver==vers)
                break;
            free(ver->data);
            link_remove(ver, 2);
            free(ver);
        }
        free(vers->data);
        free(vers);
    link_remove(obj, 3);
    free(obj);
    lock_release(&(reg->lockMem));
    return true;
}
