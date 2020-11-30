#include "help.hpp"

void Batcher::enter(bool is_ro) {
    shared_lock<shared_mutex> lock_cv{this->cv_change};
    if (unlikely(this->wait && !is_ro)) {
        this->cv.wait(lock_cv, [=]{return !this->wait;});
    }
    this->wait = true;
    this->remaining++;
    lock_cv.unlock();
    return;
}

void Batcher::leave() {
    unique_lock<shared_mutex> lock_cv{this->cv_change};
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
    this->batcher = new Batcher(this);
}

void Region::end_epoch() {
    //this->count_end++;
    if (likely(this->written.size!=0)) {
        for (auto ptr = this->written.tail.load(); ptr != nullptr; ptr = ptr->prev) {
            ptr->data->read_version = !ptr->data->read_version;
            ptr->data->access.store(-1);
            // ptr->data.second->read_version = !ptr->data.second->read_version;
            // ptr->data.second->access.store(-1);
        }
        this->written.destroy();
    }
    if (unlikely(this->to_free.size!=0)) {
        for (auto ptr = this->to_free.tail.load(); ptr != nullptr; ptr = ptr->prev) {
            void* word = ptr->data;
            size_t seg_size = this->memory_sizes[word];
            for (size_t i = 0; i < seg_size; i+=this->align) {
                delete this->memory[word+i].second;
                this->memory.erase(word+i);
            }
            this->memory_sizes.erase(word);
            free(word);
        }
        this->to_free.destroy();
    }
}

Transaction::~Transaction() {
    if (unlikely(this->failed)) {
        if (unlikely(this->first_allocs.size()!=0)) {
            //cout << "Unallocating an allocated segment by this transaction" << endl;
            for (auto const& word: this->first_allocs)
                free(word);
        }
        for (auto const& write: this->writes) {
            //write->written.store(false);
            write->access.store(-1);
            //write->commit_write.store(false);
            // write->read_tran.store(-1);
            // write->write_tran.store(-1);
        }
    }
    //this->alloc_size.clear();
    this->first_allocs.clear();
    this->allocated.clear();
    this->frees.clear();
    this->writes.clear();
    return;
}