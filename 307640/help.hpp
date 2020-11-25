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
#include <cmath>
#include <condition_variable>

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

class Region;

class Batcher {
public:
    atomic_int remaining;
    atomic_uint blocked;
    atomic_bool waiting;
    condition_variable_any cv;
    shared_mutex cv_change;
    mutex block_rem;
    Region* reg;
    Batcher(Region* reg);
    ~Batcher();
    Batcher(const Batcher&) = delete;
    Batcher& operator=(const Batcher&) = delete; 
    Batcher(Batcher&&) = delete;
    Batcher& operator=(Batcher&&) = delete;

public:
    void enter();
    void leave();
};

class WordControl {
public:
    shared_mutex lock_read;
    shared_mutex lock_write;
    atomic_bool read_version;
    atomic_int write_tran;
    atomic_int read_tran;
    WordControl();
    ~WordControl();
    WordControl(const WordControl&) = delete;
    WordControl& operator=(const WordControl&) = delete; 
    WordControl(WordControl&&) = delete;
    WordControl& operator=(WordControl&&) = delete;
};

struct hash_ptr {
    size_t operator()(const void* val) const {
        static const size_t shift = (size_t)log2(1 + sizeof(val));
        return (size_t)(val) >> shift;
    }
};

class Transaction {
public:
    int t_id;
    bool is_ro;
    unordered_map<void*, pair<void*, WordControl*>, hash_ptr> allocated;
    vector<pair<void*, size_t>> alloc_size;
    vector<void*> frees;
    Transaction(int t_id, bool is_ro);
    ~Transaction();
};

// struct hash_pair { 
//     size_t operator()(const pair<shared_ptr<WordLock>, shared_ptr<MemorySegment>>& p) const
//     { 
//         static const size_t shift1 = (size_t)log2(1 + sizeof(p.first.get()));
//         auto hash1 = (size_t)(p.first.get()) >> shift1;
//         static const size_t shift2 = (size_t)log2(1 + sizeof(p.second.get()));
//         auto hash2 = (size_t)(p.second.get()) >> shift2;
//         return hash1 ^ hash2; 
//     } 
// };


class Region {
public:
    Batcher* batcher;
    unordered_map<void*, pair<void*, WordControl*>, hash_ptr> memory;
    unordered_map<void*, size_t, hash_ptr> memory_sizes;
    list<void*> to_free;
    unordered_map<void*, pair<void*, WordControl*>, hash_ptr> to_allocate;
    mutex lock_alloc;
    mutex lock_free;
    void* first_word;
    atomic_uint tran_counter;
    size_t size;
    size_t align;
    Region(size_t size, size_t align);
    ~Region(); 
public:
    void end_epoch();
};