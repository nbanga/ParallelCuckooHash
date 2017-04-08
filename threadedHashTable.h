#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <limits.h>
#include <pthread.h>

typedef struct node{
    char *key;
    char *value;
    struct node* next;
}entryNode;

typedef struct{
    entryNode* firstNode;
    pthread_mutex_t bucketLock;
}bucketNode;

typedef struct{
    bucketNode** buckets;
    int num_entries;
    int num_buckets;
    float max_load_factor;
    float curr_load_factor;
}hashTable;

extern hashTable* hashtable;

extern char* get(char*);
extern void put(char*, char*);
extern bool removeKey(char*);
extern void resize();
extern int getSize();
extern float getLoadFactor();
extern void createHashTable(int, float);
extern entryNode* createEntryNode(char*, char*);
extern void insertEntryNode(entryNode*);



