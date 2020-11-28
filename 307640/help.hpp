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
//#include <lockfreelist.hpp>

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

template<typename T>
struct node
{
    T data;
    node* prev;
    node(T data) : data(data), prev(nullptr) {}
};
 
template<typename T>
class LockFreeList
{
public:
    atomic<node<T>*> tail;
    atomic_uint size;
    LockFreeList(): tail(nullptr), size(0) {}
    void add(T data){
        node<T>* new_node = new node<T>(data);
        auto prevTail = tail.load(std::memory_order_relaxed);
        do {
            new_node->prev = prevTail;
        } while (!tail.compare_exchange_weak(prevTail, new_node, std::memory_order_release, std::memory_order_relaxed));
        size++;
    }
    void destroy() {
        node<T>* current = tail.load();
        node<T>* prev;
        while (current != nullptr)  { 
            prev = current->prev;
            delete current; 
            current = prev; 
        }
        this->tail=nullptr;
        this->size = 0;
    }
};

class Region;

class Batcher {
public:
    atomic_int remaining;
    atomic_bool wait;
    condition_variable_any cv;
    shared_mutex cv_change;
    Region* reg;
    Batcher(Region* reg): reg(reg), remaining(0), wait(false) {}
    //~Batcher();
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
    atomic_bool read_version;
    //atomic_bool written;
    atomic_int access;
    //atomic_bool commit_write;
    //atomic_int read_tran;
    //atomic_int write_tran;
    WordControl(): read_version(false), access(-1) {}
    //~WordControl();
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
    //unordered_map<void*, pair<void*, WordControl*>, hash_ptr> allocated;
    list<void*> allocated;
    list<void*> first_allocs;
    vector<void*> frees;
    unordered_set<WordControl*, hash_ptr> writes;
    bool failed;
    Transaction(int t_id, bool is_ro): t_id(t_id), is_ro(is_ro), failed(false) {}
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
    shared_mutex lock_mem;
    unordered_map<void*, pair<void*, WordControl*>, hash_ptr> memory;
    unordered_map<void*, size_t, hash_ptr> memory_sizes;
    LockFreeList<WordControl*> written;
    LockFreeList<void*> to_free;
    void* first_word;
    atomic_uint tran_counter;
    size_t size;
    size_t align;
    atomic_uint count_end;
    Region(size_t size, size_t align);
    //~Region(); 
public:
    void end_epoch();
};