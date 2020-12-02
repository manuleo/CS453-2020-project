#include "help.hpp"

void Batcher::enter(bool is_ro) {
    shared_lock<shared_mutex> lock_cv{this->cv_change};
    if (unlikely(this->wait && !is_ro)) {
        this->cv.wait(lock_cv, [=]{return !this->wait;});
    }
    this->remaining++;
    lock_cv.unlock();
    return;
}

void Batcher::leave(bool failed) {
    unique_lock<shared_mutex> lock_cv{this->cv_change};
     if (failed)
         this->wait = true;
    int expected = 1;
    if (unlikely(this->remaining.compare_exchange_strong(expected, 0))) {
        this->reg->end_epoch();
        this->wait = false;
        lock_cv.unlock();
        this->cv.notify_all();
    } else {
        this->remaining--;
        lock_cv.unlock();
    }
    return;
}

Region::Region(size_t size, size_t align) {
    this->size = size;
    this->align = align;
    this->tran_counter.store(0);
    this->tot_size = 0;
    this->batcher = new Batcher(this);
}

void Region::end_epoch() {
    if (likely(this->written.size!=0)) {
        for (auto ptr = this->written.tail.load(); ptr != nullptr; ptr = ptr->prev) {
            atomic_uint* access = (atomic_uint*) (ptr->data + 2*this->align);
            bool* read_version = (bool*)(ptr->data + 2*this->align + sizeof(atomic_uint) + 3);
            *read_version = !(*read_version);
            access->store(0);
        }
        this->written.destroy();
    }
}