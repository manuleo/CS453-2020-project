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
    uint expected = 0;
    if (likely(!this->remaining.compare_exchange_strong(expected, 1))) {
        shared_lock<shared_mutex> lock_cv{this->cv_change};
        this->blocked++;
        if (unlikely(!this->waiting.load()))
            this->waiting.store(true);
        this->cv.wait(lock_cv, [=]{return this->waiting.load();});
    }
    return;
}

void Batcher::leave() {
    lock_guard<shared_mutex> lock_cv{this->cv_change};
    this->remaining--;
    if (this->remaining.load() == 0) {
        this->reg->end_epoch();
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
        if (likely(word_struct.second.second->write_tran.load()!=-1)) {
            bool read_ver = word_struct.second.second->read_version.load();
            word_struct.second.second->read_version.store(!read_ver);
            word_struct.second.second->write_tran.store(-1);
        }
        word_struct.second.second->read_tran.store(-1);
    }
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

Transaction::Transaction(int t_id, bool is_ro) {
    this->t_id = t_id;
    this->is_ro = is_ro;
    return;
}

Transaction::~Transaction() {
    return;
}