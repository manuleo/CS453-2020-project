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

// Internal headers
#include <tm.h>
#include <stdatomic.h>


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

/** Clean the given lock up.
 * @param lock Lock to clean up
**/
static void lock_cleanup(struct lock_t* lock as(unused)) {
    return;
}

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

typedef struct {
    int tId;
    liveNode* next;
    liveNode* prev;
} liveNode;

typedef struct {
    int readId;
    readNode* next;
    readNode* prev;
} readNode;

typedef struct {
    int ts;
    void* data;
    readNode* readList;
    versionTuple* next;
    versionTuple* prev;
} versionTuple;

typedef struct {
    struct lock_t lock;
    versionTuple* versions;
    memory* next;
    memory* prev;
} memory;

typedef struct {
    memory* object;
    writeSet* next;
    writeSet* prev;
} writeSet;

typedef struct {
    int tId;
    bool is_ro;
    writeSet* write;
    transactionObj* next;
    transactionObj* prev;
} transactionObj;

typedef struct {
    int tCount;
    struct lock_t lockList;
    liveNode* liveList;
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
        liveNode* link = (liveNode*) pointer; 
        link->prev = NULL;
        link->next = NULL;
        break;
    case 2:
        readNode* link = (readNode*) pointer; 
        link->prev = NULL;
        link->next = NULL;
        break;
    case 3:
        versionTuple* link = (versionTuple*) pointer; 
        link->prev = NULL;
        link->next = NULL;
        break;
    case 4:
        memory* link = (memory*) pointer; 
        link->prev = NULL;
        link->next = NULL;
        break;
    case 5:
        writeSet* link = (writeSet*) pointer; 
        link->prev = NULL;
        link->next = NULL;
        break;
    case 6:
        transactionObj* link = (transactionObj*) pointer; 
        link->prev = NULL;
        link->next = NULL;
        break;
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
        liveNode* link = (liveNode*) pointer; 
        liveNode* base = (liveNode*) base_void; 
        link->next = base->next;
        base->next = link;
        link->prev = base;
        if (link->next!=NULL)
            link->next->prev = link;
        break;
    case 2:
        readNode* link = (readNode*) pointer; 
        readNode* base = (readNode*) base_void; 
        link->next = base->next;
        base->next = link;
        link->prev = base;
        if (link->next!=NULL)
            link->next->prev = link;
        break;
    case 3:
        versionTuple* link = (versionTuple*) pointer; 
        versionTuple* base = (versionTuple*) base_void; 
        link->next = base->next;
        base->next = link;
        link->prev = base;
        if (link->next!=NULL)
            link->next->prev = link;
        break;
    case 4:
        memory* link = (memory*) pointer; 
        memory* base = (memory*) base_void; 
        link->next = base->next;
        base->next = link;
        link->prev = base;
        if (link->next!=NULL)
            link->next->prev = link;
        break;
    case 5:
        writeSet* link = (writeSet*) pointer; 
        writeSet* base = (writeSet*) base_void; 
        link->next = base->next;
        base->next = link;
        link->prev = base;
        if (link->next!=NULL)
            link->next->prev = link;
        break;
    case 6:
        transactionObj* link = (transactionObj*) pointer; 
        transactionObj* base = (transactionObj*) base_void; 
        link->next = base->next;
        base->next = link;
        link->prev = base;
        if (link->next!=NULL)
            link->next->prev = link;
        break;
    }
}

/** Link removal.
 * @param pointer Link to remove
**/
static void link_remove(void* pointer, int type) {
    switch (type)
    {
    case 1: 
        liveNode* prev = ((liveNode*) pointer)->prev;
        liveNode* next = ((liveNode*) pointer)->next;
        prev->next = next;
        next->prev = prev;
        break;
    case 2:
        readNode* prev = ((readNode*) pointer)->prev;
        readNode* next = ((readNode*) pointer)->next;
        prev->next = next;
        next->prev = prev;
        break;
    case 3:
        versionTuple* prev = ((versionTuple*) pointer)->prev;
        versionTuple* next = ((versionTuple*) pointer)->next;
        prev->next = next;
        next->prev = prev;
        break;
    case 4:
        memory* prev = ((memory*) pointer)->prev;
        memory* next = ((memory*) pointer)->next;
        prev->next = next;
        next->prev = prev;
        break;
    case 5:
        writeSet* prev = ((writeSet*) pointer)->prev;
        writeSet* next = ((writeSet*) pointer)->next;
        prev->next = next;
        next->prev = prev;
        break;
    case 6:
        transactionObj* prev = ((transactionObj*) pointer)->prev;
        transactionObj* next = ((transactionObj*) pointer)->next;
        prev->next = next;
        next->prev = prev;
        break;
    }
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
    // if (unlikely(posix_memalign(&(reg->start), align, size) != 0)) {
    //     free(reg);
    //     return invalid_shared;
    // }
    memory* first = (memory*) malloc(sizeof(memory));
    if (unlikely(!lock_init(&(first->lock)))) {
        free(reg);
        free(first);
        return invalid_shared;
    }
    versionTuple* first_tuple = (versionTuple*) malloc(sizeof(versionTuple));
    if (unlikely(posix_memalign(&(first_tuple->data), align, size) != 0)) {
        free(reg);
        free(first);
        free(first_tuple);
        return invalid_shared;
    }
    memset(first_tuple->data, 0, size);
    first_tuple -> ts = 0;
    first_tuple -> readList = NULL;
    link_reset(first_tuple, 3);
    first->versions = first_tuple;
    link_reset(first, 4);
    if (unlikely(!lock_init(&(reg->lockList)))) {
        free(reg);
        free(first);
        free(first_tuple);
        return invalid_shared;
    }
    reg->liveList = NULL;
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
    liveNode* lives = reg->liveList;
    if (lives!=NULL) {
        while (true) {
            liveNode* live = lives->next;
            if (live == lives)
                break;
            link_remove(live, 1);
            free(live);
        }
        free(lives);
    }
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
                    link_remove(write, 5);
                    free(write);
                }
                free(writes);
            }
            if (tran == lives)
                break;
            link_remove(tran, 6);
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
                    link_remove(read, 2);
                    free(read);
                }
                free(reads);
            }
            if(ver==vers)
                break;
            link_remove(ver, 3);
            free(ver);
        }
        free(vers);
        if (m == memories)
            break;
        link_remove(m, 4);
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
tx_t tm_begin(shared_t shared as(unused), bool is_ro as(unused)) {
    // TODO: tm_begin(shared_t)
    return invalid_tx;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared as(unused), tx_t tx as(unused)) {
    // TODO: tm_end(shared_t, tx_t)
    return false;
}

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read(shared_t shared as(unused), tx_t tx as(unused), void const* source as(unused), size_t size as(unused), void* target as(unused)) {
    // TODO: tm_read(shared_t, tx_t, void const*, size_t, void*)
    return false;
}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t shared as(unused), tx_t tx as(unused), void const* source as(unused), size_t size as(unused), void* target as(unused)) {
    // TODO: tm_write(shared_t, tx_t, void const*, size_t, void*)
    return false;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
alloc_t tm_alloc(shared_t shared as(unused), tx_t tx as(unused), size_t size as(unused), void** target as(unused)) {
    // TODO: tm_alloc(shared_t, tx_t, size_t, void**)
    return abort_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared as(unused), tx_t tx as(unused), void* target as(unused)) {
    // TODO: tm_free(shared_t, tx_t, void*)
    return false;
}
