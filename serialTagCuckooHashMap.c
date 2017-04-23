#include "serialTagCuckooHashMap.h"

cuckooHashTable* cuckoohashtable;

cuckooHashTable* createHashTable(int num_buckets){

    cuckooHashTable* hashtable = (cuckooHashTable *) malloc(sizeof(cuckooHashTable));
    hashtable->buckets = (tagNode **) malloc(num_buckets * sizeof(tagNode *));

    int i = 0, j = 0;
    for(; i<num_buckets; i++){
        hashtable->buckets[i] = (tagNode *) malloc (NUM_SLOTS * sizeof(tagNode));
        for(j = 0; j<NUM_SLOTS; j++){
            hashtable->buckets[i][j].entryNodeptr = NULL;
        }
    }

    hashtable->num_buckets = num_buckets;
    return hashtable;
}

void computehash(char* key, uint32_t *h1, uint32_t *h2){
    hashlittle2((void *)key, strlen(key), h1, h2);
}

unsigned char hashTag(const uint32_t hashValue){
    uint32_t tag = hashValue & TAG_MASK;
    return (unsigned char) tag + (tag==0);
}

void printHashTable(void){
    printf("Print Hash Table\n");
    int buckets = cuckoohashtable->num_buckets;
    int i,j;
    tagNode *newNode;

    for(i=0; i<buckets; i++){
         newNode = cuckoohashtable->buckets[i];
         for(j=0; j<NUM_SLOTS;j++){
             if(newNode[j].entryNodeptr != NULL){
                 printf("%c %s;",newNode[j].tag, newNode[j].entryNodeptr->key);
             }
         }
    }
}

char* get(char* key){
    uint32_t h1 = 0, h2 = 0;
    unsigned char tag;
    tagNode* temp;
    int i;    

    computehash(key, &h1, &h2);
    printf("GET H1: %d, H2: %d, key: %s\n", h1, h2, key);
    tag = hashTag(h1);    
    
    h1 = h1 % cuckoohashtable->num_buckets;
    temp = cuckoohashtable->buckets[h1];    
    for(i = 0; i< NUM_SLOTS; i++){
       if(temp[i].tag == tag && temp[i].entryNodeptr!=NULL){
           entryNode* entrynode = temp[i].entryNodeptr;
           if(!strcmp(key,entrynode->key)){
               printf("Get : Key : %s at [%d][%d]\n", temp[i].entryNodeptr->key, h1, i);
               return entrynode->value;
           }
       }
    } 

    h2 = h2 % cuckoohashtable->num_buckets;
    temp = cuckoohashtable->buckets[h2];    
    for(i = 0; i< NUM_SLOTS; i++){
       if(temp[i].tag == tag && temp[i].entryNodeptr!=NULL){
           entryNode* entrynode = temp[i].entryNodeptr;
           if(!strcmp(key,entrynode->key)){
               printf("Get : Key : %s at [%d][%d]\n", temp[i].entryNodeptr->key, h1, i);
               return entrynode->value;
           }
       }
    }
    
    printf("Get : NOT Found key : %s\n", key);
    return NULL;
}

bool removeKey(char* key){
    uint32_t h1 = 0, h2 = 0;
    tagNode* temp;
    int i;    
    unsigned char tag;

    computehash(key, &h1, &h2);
    tag = hashTag(h1);
    
    h1 = h1 % cuckoohashtable->num_buckets;
    temp = cuckoohashtable->buckets[h1];    
    for(i = 0; i< NUM_SLOTS; i++){
       if(temp[i].tag == tag && temp[i].entryNodeptr!=NULL){
           entryNode* entrynode = temp[i].entryNodeptr;
           if(!strcmp(entrynode->key, key)){
               free(entrynode->key);
               free(entrynode->value);
               free(entrynode);
               temp[i].entryNodeptr=NULL;
               return true;
           }
       }
    } 

    h2 = h2 % cuckoohashtable->num_buckets;
    temp = cuckoohashtable->buckets[h2];    
    for(i = 0; i< NUM_SLOTS; i++){
        if(temp[i].tag == tag && temp[i].entryNodeptr!=NULL){
           entryNode* entrynode = temp[i].entryNodeptr;
           if(!strcmp(entrynode->key, key)){
               free(entrynode->key);
               free(entrynode->value);
               free(entrynode);
               temp[i].entryNodeptr=NULL;
               return true;
           }
       } 
    }
    
    return false;
}

void freeHashTable(cuckooHashTable* newHashTable){
    int i;

    for(i=0; i<newHashTable->num_buckets; i++)
        free(newHashTable->buckets[i]);

    free(newHashTable);
    return;
}

entryNode* _put(cuckooHashTable* htptr, char *key, char *value){
    int num_iterations = 0;
    uint32_t h1 = 0, h2 = 0;
    tagNode *first, *second;
    tagNode evictentry;
    char *curr_key, *curr_value;
    unsigned char tag;
    int i;

    curr_key = key;
    curr_value = value;
    char* temp_key = (char *) malloc(sizeof(char) * MAX_SIZE);
    char* temp_value = (char *) malloc(sizeof(char) * MAX_SIZE);
    
    while(num_iterations < MAX_ITERATIONS){
        printf("Num Iterations %d for key %s\n", num_iterations, key);
        num_iterations++;
        computehash(curr_key, &h1, &h2);
        printf("PUT H1: %d, H2: %d, key: %s\n", h1, h2, curr_key);

        h1 = h1 % htptr->num_buckets;
        first = htptr->buckets[h1];    
        h2 = h2 % htptr->num_buckets;
        second = htptr->buckets[h2];
        tag = hashTag(h1);
    
        for(i = 0; i< NUM_SLOTS; i++){
            if (first[i].entryNodeptr == NULL){
                entryNode* entrynode = (entryNode*) malloc(1*sizeof(entryNode));
                entrynode->key = (char*) malloc(MAX_SIZE*sizeof(char));
                entrynode->value = (char*) malloc(MAX_SIZE*sizeof(char));
                strcpy(entrynode->key,curr_key);
                strcpy(entrynode->value,curr_value);
                first[i].tag = tag;
                first[i].entryNodeptr = entrynode;
                printf("First: Inserted Key %s in bucket %d, %d\n", key, h1, i); 
                return NULL;
            }
            else if(!strcmp(first[i].entryNodeptr->key,curr_key)){
                strcpy(first[i].entryNodeptr->value,curr_value);
                return NULL;
            }
        }
        
        for(i = 0; i< NUM_SLOTS; i++){
            if (second[i].entryNodeptr == NULL){
                entryNode* entrynode = (entryNode*) malloc(1*sizeof(entryNode));
                entrynode->key = (char*) malloc(MAX_SIZE*sizeof(char));
                entrynode->value = (char*) malloc(MAX_SIZE*sizeof(char));
                strcpy(entrynode->key,curr_key);
                strcpy(entrynode->value,curr_value);
                second[i].tag = tag;
                second[i].entryNodeptr = entrynode;
                printf("Second: Inserted Key %s in bucket %d, %d\n", key, h2, i); 
                return NULL;
            }
            else if(!strcmp(second[i].entryNodeptr->key,curr_key)){
                strcpy(second[i].entryNodeptr->value,curr_value);
                return NULL;
            }
        }

        int index = rand() % (2*NUM_SLOTS);
        if(index >= 0 && index <NUM_SLOTS){
            evictentry = first[index];
        }
        else{
            evictentry = second[index - NUM_SLOTS];
        }

        printf("Evicted Key %s with index %d\n", evictentry.entryNodeptr->key, index);
        strcpy(temp_key, evictentry.entryNodeptr->key);
        strcpy(temp_value, evictentry.entryNodeptr->value);

        evictentry.tag = tag;
        strcpy(evictentry.entryNodeptr->key, curr_key);
        strcpy(evictentry.entryNodeptr->value, curr_value);
        printHashTable();
        
        strcpy(curr_key, temp_key);
        strcpy(curr_value, temp_value);
    }
    
    entryNode* finalevictedNode = (entryNode *) malloc(sizeof(entryNode));
    finalevictedNode->key = curr_key;
    finalevictedNode->value = curr_value;
    return finalevictedNode;
}

void resize(int num_buckets){
    printf("In Resize\n");
    cuckooHashTable *newHashTable = createHashTable(num_buckets);
    int i, j;
    entryNode* res;

    for(i = 0; i < cuckoohashtable->num_buckets; i++){
        tagNode *bucketRow = cuckoohashtable->buckets[i]; 
        for(j=0; j < NUM_SLOTS; j++){
            if(bucketRow[j].entryNodeptr != NULL){
                res = _put(newHashTable, bucketRow[j].entryNodeptr->key, bucketRow[j].entryNodeptr->value);
                if(res != NULL){
                    break;
                }
            }
        }
        if(res != NULL)
            break;        
    }
    if(res != NULL){
        printf("Inside Resize: Free and Resize\n");
        printHashTable();
        freeHashTable(newHashTable);
        resize(2 * num_buckets);
        _put(cuckoohashtable, res->key, res->value);
    }
    freeHashTable(cuckoohashtable);
    cuckoohashtable = newHashTable;
}

void put(char* key, char* value){
    entryNode* res = _put(cuckoohashtable, key, value);
   
    if(res != NULL){
        printHashTable();
        resize((cuckoohashtable->num_buckets) * 2);
        _put(cuckoohashtable, res->key, res->value);
        printHashTable();
    }
    
    printf("Put a New value : %s\n", value);
    return;
}


int main(void){
    cuckoohashtable = createHashTable(10);
    printf("createHashTable: num_buckets %d\n", cuckoohashtable->num_buckets);

    char** keys = (char**)malloc(100*sizeof(char*)); 
    printf("allocated keys\n");
    char* value = (char*)malloc(100*sizeof(char)); 
    printf("allocated values\n");
    
    int i;
    for(i=0;i<100;i++){
        keys[i] = (char*)malloc(10*sizeof(char));
        //printf("allocated key %d\n",i);
        snprintf(keys[i],10,"%d",i);
        //printf("key%d: %s\n", i, keys[i]);
        snprintf(value,20,"%d%s",i,"values");
        //printf("value: %s\n",value);
        put(keys[i],value);
        //printf("put in entry %d\n", i);
    }
    
    printf("num_entries after put = %d\n", cuckoohashtable->num_buckets);
    printf("Get\n");
    
    printHashTable();
    printf("\n");

  /* for(i=0;i<20;i++){
        printf("key: %s, value: %s\n", keys[i],get(keys[i]));
    }*/
    

    printf("testing resize\n");
    for(i=11;i<20;i++){
        keys[i] = (char*)malloc(10*sizeof(char));
        snprintf(keys[i],10,"%d",i);
        snprintf(value,20,"%d%s",i,"values");
        put(keys[i],value);
    }
    
    printf("createHashTable: num_buckets %d\n", cuckoohashtable->num_buckets);

    for(i=0;i<5;i++){
        printf("remove key %s : %d\n", keys[i], removeKey(keys[i]));
    }


   printf("remove key %s : %d\n", "101", removeKey("101"));
   return 0;
}
