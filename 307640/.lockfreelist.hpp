// A lock-free single linked list
#include<atomic>
template<typename T>
struct node
{
    T data;
    node* prev;
    node(const T& data) : data(data), prev(nullptr) {}
};
 
template<typename T>
class LockFreeList
{
public:
    std::atomic<node<T>*> tail;
    LockFreeList();
    ~LockFreeList();
    void add(const T& data);
    void destroy();
    void reset_tail();
};