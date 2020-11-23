#include "help.hpp"

Batcher::Batcher() {
    this->blocked.store(0);
    this->remaining.store(0);
    this->waiting.store(false);
    return;
}

Batcher::~Batcher() {
    return;
}

void Batcher::enter() {
    uint expected = 0;
    if (likely(!this->remaining.compare_exchange_strong(expected, 1))) {
        shared_lock<shared_mutex> lock_cv{this->cv_change};
        this->blocked++;
        if (unlikely(!this->waiting.load()))
            this->waiting.store(true);
        this->cv.wait(lock_cv, [=]{return !this->waiting.load();});
    }
    return;
}

void Batcher::leave() {
    lock_guard<shared_mutex> lock_cv{this->cv_change};
    this->remaining--;
    if (this->remaining.load() == 0) {
        this->remaining.store(blocked.load());
        this->blocked.store(0);
        this->waiting.store(false);
        this->cv.notify_all();
    }
    return;
}

Region::Region(size_t size, size_t align) {
    this->size = size;
    this->align = align;
    this->tran_counter.store(0);
}

Region::~Region() {
    return;
}

WordControl::WordControl() {
    this->read_version.store(0);
    this->write_tran.store(-1);
    this->read_tran.store(-1);
    return;
}

WordControl::~WordControl() {
    return;
}

Transaction::Transaction(uint t_id, bool is_ro) {
    this->t_id = t_id;
    this->is_ro = is_ro;
    return;
}

Transaction::~Transaction() {
    return;
}