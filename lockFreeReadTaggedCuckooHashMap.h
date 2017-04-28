#include "header.h"

#define NUM_SLOTS 4
#define MAX_ITERATIONS 15
#define MAX_SIZE 1024
#define TAG_MASK 0xFF
#define VERSION_COUNTER_SIZE 8192

typedef struct enode{
    char *key;
    char *value;
}entryNode;

typedef struct tnode{
    unsigned char tag;
    entryNode* entryNodeptr;
}tagNode;

typedef struct{
    tagNode** buckets;
    int num_buckets; 
    pthread_mutex_t write_lock;
    int* keys_accessed_bitmap;
    uint32_t* version_counter;
    pthread_rwlock_t hashTableLock; 
}cuckooHashTable;


extern cuckooHashTable* cuckoohashtable;

extern char* get(char*);
extern void put(char*, char*, int);
extern bool _put(char*, char*);
extern bool removeKey(char*);
extern void rehash();
extern void resize();
extern cuckooHashTable* createHashTable(int);
extern void computeHash(const char*, uint32_t *, uint32_t*);
extern unsigned char hashTag(uint32_t);
extern void evictEntriesFromPath(int*, int*, int, int, char*, char*); 
