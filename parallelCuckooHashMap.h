#include "header.h"

#define NUM_SLOTS 4
#define MAX_ITERATIONS 15
#define MAX_SIZE 1024

typedef enum{HASHLITTLE2, HASHWORD2} hash_function;

typedef struct node{
    char *key;
    char *value;
}entryNode;

typedef struct{
    entryNode* firstNode;
    pthread_mutex_t bucketLock;
}bucketNode;

typedef struct{
    bucketNode* buckets;
    int num_buckets;
    int num_entries;
    hash_function hashFunction;
    pthread_rwlock_t hashTableLock; 
}cuckooHashTable;

extern cuckooHashTable* cuckoohashtable;

extern char* get(char*);
extern void put(char*, char*);
extern entryNode* _put(cuckooHashTable*, char*, char*);
extern bool removeKey(char*);
extern void resize();
extern cuckooHashTable* createHashTable(int);
extern void computeHash(const char*, uint32_t *, uint32_t*);
