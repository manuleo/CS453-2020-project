#include "help.hpp"

void Batcher::enter(bool is_ro) {
    shared_lock<shared_mutex> lock_cv{this->cv_change};
    if (unlikely(this->wait.load() && !is_ro)) {
        this->cv.wait(lock_cv, [=]{return !this->wait.load();});
    }
    if (unlikely(!this->wait))
        this->wait.store(true);
    this->remaining++;
    lock_cv.unlock();
    return;
}

void Batcher::leave(bool failed) {
    unique_lock<shared_mutex> lock_cv{this->cv_change};
    // if (unlikely(failed))
    //     this->wait.store(true);
    int expected = 1;
    if (unlikely(this->remaining.compare_exchange_strong(expected, 0))) {
        this->reg->end_epoch();
        this->wait.store(false);
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
    if (unlikely(this->to_free.size!=0)) {
        for (auto ptr = this->to_free.tail.load(); ptr != nullptr; ptr = ptr->prev) {
            FreeControl* free_contr = ptr->data;
            if (likely(free_contr->is_valid)) {
                for (size_t i = 0; i < this->memory_sizes[free_contr->word]; i+=this->align) {
                    delete this->memory[free_contr->word+i].second;
                    this->memory.erase(free_contr->word+i);
                }
                this->memory_sizes.erase(free_contr->word);
                free(free_contr->word);
            }
            delete free_contr;
        }
        this->to_free.destroy();
    }
    if (likely(this->written.size!=0)) {
        for (auto ptr = this->written.tail.load(); ptr != nullptr; ptr = ptr->prev) {
            if (likely(ptr->data->commit_write)) {
                bool read_ver = ptr->data->read_version.load();
                ptr->data->read_version.store(!read_ver);
                ptr->data->access.store(-1);
            }
            ptr->data->commit_write.store(false);
        }
        this->written.destroy();
    }
    // int sum = 0;
    // for (auto const& word_struct: this->memory) {
    //     void* read_copy = word_struct.second.first + (word_struct.second.second->read_version ? this->align : 0);
    //     int val = *(int*)read_copy;
    //     sum+=val;
    //     //cout << "Word: " << word_struct.first << " Val:" << val << endl;
    // }
    // cout << "Total sum: " << sum << endl;

//    if (this->batcher->remaining!=0)
//        cout << "Remaining: " << this->batcher->remaining << endl;
}

Transaction::~Transaction() {
    if (unlikely(this->failed)) {
        if (unlikely(this->first_allocs.size()!=0)) {
            for (auto const& word: this->first_allocs)
                free(word);
        }
        if (unlikely(this->frees.size()!=0)) {
            for (auto const& free_contr: this->frees)
                free_contr->is_valid = false;
        }
        for (auto const& write: this->writes) {
            //write->written.store(false);
            write->access.store(-1);
            write->commit_write.store(false);
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