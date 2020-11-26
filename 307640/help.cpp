#include "help.hpp"

Batcher::Batcher(Region* reg) {
    this->blocked.store(0);
    this->remaining.store(0);
    this->waiting.store(false);
    this->reg = reg;
    return;
}

Batcher::~Batcher() {
    return;
}

void Batcher::enter() {
    shared_lock<shared_mutex> lock_cv{this->cv_change};
    int expected = 0;
    if (likely(!this->remaining.compare_exchange_strong(expected, 1, memory_order_acquire, memory_order_relaxed))) {
        // unique_lock<mutex> lock_block_rem{this->block_rem};
        // this->blocked++;
        // if (unlikely(!this->waiting.load()))
        //     this->waiting.store(true);
        // lock_block_rem.unlock();
        // this->cv.wait(lock_cv, [=]{return !this->waiting.load();});
        this->cv.wait(lock_cv, [=]{return this->remaining==0;});
        lock_cv.unlock();
        this->remaining++;
    }
    return;
}

void Batcher::leave() {
    this->cv_change.lock();
    //this->remaining--;
    int expected = 1;
    if (this->remaining.compare_exchange_strong(expected, 0)) {
        this->reg->end_epoch();
        // unique_lock<mutex> lock_block_rem{this->block_rem};
        // this->remaining.store(blocked.load());
        // this->blocked.store(0);
        // this->waiting.store(false);
        // lock_block_rem.unlock();
        this->cv_change.unlock();
        this->cv.notify_all();
    } else {
        while (!(this->remaining.compare_exchange_strong(expected, expected-1)));
        if (expected == 1) {
            this->reg->end_epoch();
            this->cv_change.unlock();
            this->cv.notify_all();
        } else {
            this->cv_change.unlock();
        }
    }
    return;
}

Region::Region(size_t size, size_t align) {
    this->size = size;
    this->align = align;
    this->tran_counter.store(0);
    this->batcher = new Batcher(this);
}

Region::~Region() {
    return;
}

void Region::end_epoch() {
    if (unlikely(this->to_allocate.size()!=0)) {
        for (auto const& pair: this->to_allocate)
            this->memory[pair.first] = pair.second;
        this->to_allocate.clear();
    }
    if (unlikely(this->to_free.size()!=0)) {
        for (auto const& word: this->to_free) {
            for (size_t i = 0; i < this->memory_sizes[word]; i+=this->align) {
                delete this->memory[word+i].second;
                this->memory.erase(word+i);
            }
            this->memory_sizes.erase(word);
            free(word);
        }
        this->to_free.clear();
    }
    for (auto const& word_struct: this->memory) {
        if (likely(word_struct.second.second->commit_write.load())) {
            bool read_ver = word_struct.second.second->read_version.load();
            word_struct.second.second->read_version.store(!read_ver);
            //word_struct.second.second->written.store(false);
            word_struct.second.second->commit_write.store(false);
        }
        word_struct.second.second->read_tran.store(-1);
        word_struct.second.second->write_tran.store(-1);
    }
}

WordControl::WordControl() {
    this->read_version.store(false);
    this->write_tran.store(-1);
    this->read_tran.store(-1);
    // this->access.store(-1);
    // this->written.store(false);
    this->commit_write.store(false);
    return;
}

WordControl::~WordControl() {
    return;
}

Transaction::Transaction(int t_id, bool is_ro) {
    this->t_id = t_id;
    this->is_ro = is_ro;
    return;
}

Transaction::~Transaction() {
    return;
}