#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <limits.h>

#define NUM_SLOTS 4
#define MAX_ITERATIONS 100

typedef struct node{
    char *key;
    char *value;
}entryNode;

typedef struct{
    entryNode** buckets;
    int num_buckets;
}hashTable;

extern hashTable* hashtable;

extern char* get(char*);
extern void put(char*, char*);
extern bool removeKey(char*);
extern void resize();
extern void createHashTable(int, float);
extern entryNode* createEntryNode(char*, char*);
extern void insertEntryNode(entryNode*);
