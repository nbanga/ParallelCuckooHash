#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <limits.h>
#include "lookup3.h"

#define NUM_SLOTS 4
#define MAX_ITERATIONS 100

typedef enum{
    hashlittle2, 
    hashword2
}hash_function;

typedef struct node{
    char *key;
    char *value;
}entryNode;

typedef struct{
    entryNode** buckets;
    int num_buckets;
    int num_entries;
    float max_load_factor;
    hash_function hashFunction;
}hashTable;

extern hashTable* hashtable;

extern char* get(char*);
extern void put(char*, char*);
extern bool removeKey(char*);
extern float getLoadFactor();
extern void rehash();
extern void resize();
extern void createHashTable(int, float);
extern entryNode* createEntryNode(char*, char*);
extern void insertEntryNode(entryNode*);
