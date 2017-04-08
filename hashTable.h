#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <limits.h>

#define MAX_SIZE 1024

typedef struct node{
    char *key;
    char *value;
    struct node* next;
}entryNode;

typedef struct{
    entryNode** buckets;
    int num_entries;
    int num_buckets;
    float max_load_factor;
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



