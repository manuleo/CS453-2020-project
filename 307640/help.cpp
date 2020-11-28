#include "help.hpp"

void Batcher::enter() {
    if (likely(this->wait.load())) {
        shared_lock<shared_mutex> lock_cv{this->cv_change};
        this->cv.wait(lock_cv, [=]{return !this->wait.load();});
        lock_cv.unlock();
    }
    this->wait.store(true);
    this->remaining++;
    return;
}

void Batcher::leave() {
    unique_lock<shared_mutex> lock_cv{this->cv_change};
    int expected = 1;
    if (unlikely(this->remaining.compare_exchange_strong(expected, 0))) {
        this->reg->end_epoch();
        this->wait.store(false);
        lock_cv.unlock();
        this->cv.notify_all();
    } else {
        this->remaining--;
        lock_cv.unlock();
        // while (!(this->remaining.compare_exchange_strong(expected, expected-1)));
        // if (unlikely(expected == 1)) {
        //     this->reg->end_epoch();
        //     this->wait.store(false);
        //     lock_cv.unlock();
        //     this->cv.notify_all();
        // } else {
        //     lock_cv.unlock();
        // }
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
            void* word = ptr->data;
            for (size_t i = 0; i < this->memory_sizes[word]; i+=this->align) {
                delete this->memory[word+i].second;
                this->memory.erase(word+i);
            }
            this->memory_sizes.erase(word);
            free(word);
        }
        this->to_free.destroy();
    }
    if (likely(this->written.size!=0)) {
        for (auto ptr = this->written.tail.load(); ptr != nullptr; ptr = ptr->prev) {
            bool read_ver = ptr->data->read_version.load();
            ptr->data->read_version.store(!read_ver);
            ptr->data->access.store(-1);
        }
        this->written.destroy();
    }
    // if (this->batcher->remaining!=0)
    //     cout << "Remaining: " << this->batcher->remaining << endl;
    // for (auto const& word_struct: this->memory) {
    //     if (likely(word_struct.second.second->commit_write.load())) {
    //         bool read_ver = word_struct.second.second->read_version.load();
    //         word_struct.second.second->read_version.store(!read_ver);
    //         //word_struct.second.second->written.store(false);
    //         word_struct.second.second->commit_write.store(false);
    //         // void* read_copy = word_struct.second.first + (word_struct.second.second->read_version ? this->align : 0);
    //         // cout << "Changed word at: " << word_struct.first << " New val: " << *(int*)read_copy << endl;
    //     }
    //     word_struct.second.second->access.store(-1);
    //     // word_struct.second.second->read_tran.store(-1);
    //     // word_struct.second.second->write_tran.store(-1);
    // }
    //cout << "-----------------------End of change word---------------------------------" << endl;
    // int sum = 0;
    // for (auto const& word_struct: this->memory) {
    //     void* read_copy = word_struct.second.first + (word_struct.second.second->read_version ? this->align : 0);
    //     int val = *(int*)read_copy;
    //     if (val!=320)
    //         sum+=val;
    //     //cout << "Word: " << word_struct.first << " Val:" << val << endl;
    // }
    //cout << "Total sum: " << sum << endl;
    //cout << "------------------------End of the batch--------------------------------------" << endl;
}

Transaction::~Transaction() {
    if (unlikely(this->first_allocs.size()!=0)) {
        if (unlikely(this->failed)) {
            for (auto const& word: this->first_allocs)
                free(word);
        }
    }
    if (unlikely(this->failed)) {
        for (auto const& write: this->writes) {
            //write->written.store(false);
            write->access.store(-1);
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