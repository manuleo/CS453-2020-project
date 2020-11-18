#pragma once

// External headers
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <list>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <vector>
#include <map>
#include <unordered_map>
#include <utility>
#include <set>
#include <unordered_set>
#include <functional>

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
using std::atomic;
using std::list;
using std::set;
using std::unordered_map;
using std::unordered_set;
using std::shared_mutex;

class WordLock {
public:
    recursive_timed_mutex lock;
    atomic_uint version;
    atomic_bool is_freed;
    WordLock();
    ~WordLock();
    WordLock(const WordLock&) = delete;
    WordLock& operator=(const WordLock&) = delete; 
    WordLock(WordLock&&) = delete;
    WordLock& operator=(WordLock&&) = delete;
};


class MemorySegment {
public:
    void* data;
    size_t size;
    shared_mutex lock_pointers;
    atomic_bool is_freed;
    unordered_map<void*, shared_ptr<WordLock>> writelocks;
    MemorySegment(size_t size);
    ~MemorySegment();
};

enum class WriteType: int {
    write = 0, 
    alloc = 1, 
    free = 2,
    dummy = 3
};

class Write {
public:
    shared_ptr<WordLock> lock;
    void* data;
    shared_ptr<MemorySegment> segment;
    WriteType type;
    bool allocated;
    list<shared_ptr<WordLock>> lock_frees;
    bool will_be_freed;
    Write(shared_ptr<WordLock> lock, shared_ptr<MemorySegment> segment, WriteType type);
    ~Write();
};


struct hash_pair {
    size_t operator()(const pair<shared_ptr<WordLock>, shared_ptr<MemorySegment>> &pair ) const
    {
        return hash<shared_ptr<WordLock>>()(pair.first) ^ hash<shared_ptr<MemorySegment>>()(pair.second);
    }
};

class TransactionObject {
public:
    uint t_id;
    bool is_ro;
    uint rv;
    uint wv;
    unordered_map<void*, Write*> writes;
    vector<void*> order_writes;
    unordered_set<pair<shared_ptr<WordLock>, shared_ptr<MemorySegment>>, hash_pair> reads;
    unordered_map<void*, shared_ptr<MemorySegment>> allocated;
    bool removed;
    TransactionObject(uint t_id, bool is_ro, uint rv);
    ~TransactionObject();
};


class Region {
public:
    atomic_uint clock;
    unordered_map<void*, shared_ptr<MemorySegment>> memory;
    void* first_word;
    shared_mutex lock_mem;
    atomic_uint tran_counter;
    shared_mutex lock_trans;
    unordered_map<uint, shared_ptr<TransactionObject>> trans;
    size_t size;
    size_t align;
    Region(size_t size, size_t align);
    ~Region(); 
};