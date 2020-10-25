#pragma once

// External headers
#include <atomic>
#include <mutex>
#include <memory>
#include <list>
#include <stdlib.h>
#include <string.h>
#include <algorithm>
#include <iostream>
#include <vector>

// Requested features
#ifndef _GNU_SOURCE
    #define _GNU_SOURCE
#endif

#define _POSIX_C_SOURCE   200809L
#ifdef __STDC_NO_ATOMICS__
    #error Current C11 compiler does not support atomic operations
#endif

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

#if (defined(__i386__) || defined(__x86_64__)) && defined(USE_MM_PAUSE)
    #include <xmmintrin.h>
#else
    #include <sched.h>
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

using namespace std;
using std::vector;

// /** Your lock class.
// **/
// class Lock final {
// public:
//     /** Deleted copy/move constructor/assignment.
//     **/
//     Lock(Lock const&) = delete;
//     Lock& operator=(Lock const&) = delete;
//     // NOTE: Actually, one could argue it makes sense to implement move,
//     //       but we don't care about this feature in our simple playground
// public:
//     atomic_bool locked;
//     Lock();
//     ~Lock();
// public:
//     bool init();
//     bool lock();
//     void unlock();
// };

class VersionTuple {
public:
    int ts;
    void* data;
    vector<int> readList;
    VersionTuple(int ts, void* data);
    ~VersionTuple();
    // VersionTuple copyable/movable for now
    // VersionTuple(const VersionTuple&) = delete;
    // VersionTuple& operator=(const VersionTuple&) = delete; 
    // VersionTuple(VersionTuple&&) = delete;
    // VersionTuple& operator=(VersionTuple&&) = delete;
};

class MemoryObject {
public:
    recursive_mutex lock;
    vector<VersionTuple*> versions;
    int id_deleted;
    MemoryObject();
    ~MemoryObject();  
    MemoryObject(const MemoryObject&) = delete;
    MemoryObject& operator=(const MemoryObject&) = delete; 
    MemoryObject(MemoryObject&&) = delete;
    MemoryObject& operator=(MemoryObject&&) = delete;  
};

enum class WriteType: int {
    write = 0, 
    alloc   = 1, 
    del   = 2
};

class Write {
public:
    shared_ptr<MemoryObject> object;
    void* data;
    size_t size;
    WriteType type;
    bool read;
    Write(shared_ptr<MemoryObject> object, size_t size, WriteType type);
    ~Write(); 
    Write(const Write&) = delete;
    Write& operator=(const Write&) = delete; 
    Write(Write&&) = delete;
    Write& operator=(Write&&) = delete;  
};

class TransactionObject {
public:
    int t_id;
    bool is_ro;
    vector<shared_ptr<Write>> writes;
    vector<shared_ptr<MemoryObject>> reads;
    TransactionObject(int t_id, bool is_ro);
    ~TransactionObject(); 
    TransactionObject(const TransactionObject&) = delete;
    TransactionObject& operator=(const TransactionObject&) = delete; 
    TransactionObject(TransactionObject&&) = delete;
    TransactionObject& operator=(TransactionObject&&) = delete;  
};

class Region {
public:
    int t_count;
    recursive_mutex lock_trans;
    recursive_mutex lock_mem;
    vector<shared_ptr<MemoryObject>> memory;
    vector<shared_ptr<TransactionObject>> trans;
    size_t size;
    size_t align;
    Region(size_t size, size_t align);
    ~Region(); 
};