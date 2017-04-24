#include "lockFreeReadTaggedCuckooHashMap.h"

cuckooHashTable* cuckoohashtable;

cuckooHashTable* createHashTable(int num_buckets){

    cuckooHashTable* hashtable = (cuckooHashTable *) malloc(sizeof(cuckooHashTable));
    hashtable->buckets = (tagNode **) malloc(num_buckets * sizeof(tagNode *));
    hashtable->keys_accessed_bitmap = (int **) malloc(sizeof(int *) * num_buckets);
    
    int i = 0, j = 0;
    for(; i<num_buckets; i++){
        hashtable->buckets[i] = (tagNode *) malloc (NUM_SLOTS * sizeof(tagNode));
        hashtable->keys_accessed_bitmap[i] = (int *) malloc(sizeof(int ) * NUM_SLOTS);

        for(j = 0; j<NUM_SLOTS; j++){
            hashtable->buckets[i][j].entryNodeptr = NULL;
            hashtable->keys_accessed_bitmap[i][j] = 0;
        }
    }

    hashtable->num_buckets = num_buckets;
    hashtable->version_counter = (uint32_t *) malloc(sizeof(uint32_t) * VERSION_COUNTER_SIZE);
    pthread_mutex_init(&hashtable->write_lock, NULL);    

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
         printf("Bucket %d \t", i);
        
         newNode = cuckoohashtable->buckets[i];
         for(j=0; j<NUM_SLOTS;j++){
             if(newNode[j].entryNodeptr != NULL){
                 printf("%d %s; ",newNode[j].tag, newNode[j].entryNodeptr->key);
                 //printf("%s ", newNode[j].entryNodeptr->key);
             }
         }
         printf("\n");
    }
}

char* get(char* key){
    uint32_t h1 = 0, h2 = 0;
    unsigned char tag;
    tagNode *first, *second;
    int i, ver_index;
    uint32_t start_ver_val, end_ver_val;    

    computehash(key, &h1, &h2);
    
    tag = hashTag(h1);    
    h1 = h1 % cuckoohashtable->num_buckets;
    h2 = h2 % cuckoohashtable->num_buckets;
    first = cuckoohashtable->buckets[h1];    
    second = cuckoohashtable->buckets[h2];    
    ver_index = (h1 * NUM_SLOTS) % VERSION_COUNTER_SIZE;

    printf("GET H1: %d, H2: %d, key: %s\n", h1, h2, key);
  
    while(true){
        start_ver_val = __sync_fetch_and_add(&cuckoohashtable->version_counter[ver_index], 0);        
        
        for(i = 0; i< NUM_SLOTS; i++){
            printf("Temp1 Tag: %d, My Tag : %d\n", first[i].tag, tag);
            if(first[i].tag == tag && first[i].entryNodeptr!=NULL){
               printf("Temp Val: %s\n", first[i].entryNodeptr->key);
               entryNode* entrynode = first[i].entryNodeptr;
               if(!strcmp(key,entrynode->key)){
                   char* result = entrynode->value;
                   end_ver_val = __sync_fetch_and_add(&cuckoohashtable->version_counter[ver_index], 0);
                   if(start_ver_val != end_ver_val || (start_ver_val & 0x1)){
                        continue;
                   }
                   printf("Get : Key : %s at [%d][%d]\n", first[i].entryNodeptr->key, h1, i);
                   return result;
               }
           }
        } 
      
        for(i = 0; i< NUM_SLOTS; i++){
            printf("Temp1 Tag: %d, My Tag : %d\n", second[i].tag, tag);
            if(second[i].tag == tag && second[i].entryNodeptr!=NULL){
               printf("Temp Val: %s\n", second[i].entryNodeptr->key);
               entryNode* entrynode = second[i].entryNodeptr;
               if(!strcmp(key,entrynode->key)){
                   char* result = entrynode->value;
                   end_ver_val = __sync_fetch_and_add(&cuckoohashtable->version_counter[ver_index], 0);
                   if(start_ver_val != end_ver_val || (start_ver_val & 0x1)){
                        continue;
                   }
                   printf("Get : Key : %s at [%d][%d]\n", second[i].entryNodeptr->key, h1, i);
                   return result;
               }
           }
        }
    }

    printf("Get : NOT Found key : %s\n", key);
    return NULL;
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
    int i, ver_index;
    int eviction_path[MAX_ITERATIONS];
    int evicted_key_version[MAX_ITERATIONS];
    int evicted_node_counter = 0;
    int evicted_key_version_counter = 0;

    curr_key = key;
    curr_value = value;
    char* temp_key = (char *) malloc(sizeof(char) * MAX_SIZE);
    char* temp_value = (char *) malloc(sizeof(char) * MAX_SIZE);
    
    while(num_iterations < MAX_ITERATIONS){
        printf("Num Iterations %d for key %s\n", num_iterations, key);
        num_iterations++;
        h1 = 0; h2 = 0;
        computehash(curr_key, &h1, &h2);

        tag = hashTag(h1);    
        h1 = h1 % htptr->num_buckets;
        h2 = h2 % htptr->num_buckets;
        first = htptr->buckets[h1];    
        second = htptr->buckets[h2];
        ver_index = (h1 * NUM_SLOTS) % VERSION_COUNTER_SIZE;
        
        if(num_iterations != 1)
            evicted_key_version[evicted_key_version_counter++] = ver_index;

        printf("PUT H1: %d, H2: %d, key: %s\n", h1, h2, curr_key);

        for(i = 0; i< NUM_SLOTS; i++){
            if (first[i].entryNodeptr == NULL){
               /* entryNode* entrynode = (entryNode*) malloc(1*sizeof(entryNode));
                entrynode->key = (char*) malloc(MAX_SIZE*sizeof(char));
                entrynode->value = (char*) malloc(MAX_SIZE*sizeof(char));
                strcpy(entrynode->key,curr_key);
                strcpy(entrynode->value,curr_value);
                first[i].tag = tag;
                first[i].entryNodeptr = entrynode;
                printf("First: Inserted Key %s in bucket %d, %d with tag %d\n", key, h1, i, tag); 
                return NULL;*/
                eviction_path[evicted_node_counter++] = (h1 * NUM_SLOTS) + i;
                // TODO : SwapPathKeys
            }
            else if(num_iterations == 1 && !strcmp(first[i].entryNodeptr->key,curr_key)){
                __sync_fetch_and_add(&cuckoohashtable->version_counter[ver_index], 1);
                strcpy(first[i].entryNodeptr->value,curr_value);
                __sync_fetch_and_add(&cuckoohashtable->version_counter[ver_index], 1);
                pthread_mutex_unlock(&cuckoohashtable->write_lock);
                return NULL;
            }
        }
        
        for(i = 0; i< NUM_SLOTS; i++){
            if (second[i].entryNodeptr == NULL){
                /*entryNode* entrynode = (entryNode*) malloc(1*sizeof(entryNode));
                entrynode->key = (char*) malloc(MAX_SIZE*sizeof(char));
                entrynode->value = (char*) malloc(MAX_SIZE*sizeof(char));
                strcpy(entrynode->key,curr_key);
                strcpy(entrynode->value,curr_value);
                second[i].tag = tag;
                second[i].entryNodeptr = entrynode;
                printf("Second: Inserted Key %s in bucket %d, %d with tag %d\n", key, h2, i, tag); 
                return NULL;*/
                eviction_path[evicted_node_counter++] = (h2 * NUM_SLOTS) + i;
                // TODO : SwapPathKeys
            }
            else if(num_iterations == 1 && !strcmp(second[i].entryNodeptr->key,curr_key)){
                __sync_fetch_and_add(&cuckoohashtable->version_counter[ver_index], 1);
                strcpy(second[i].entryNodeptr->value,curr_value);
                __sync_fetch_and_add(&cuckoohashtable->version_counter[ver_index], 1);
                pthread_mutex_unlock(&cuckoohashtable->write_lock);
                return NULL;
            }
        }

        int index = rand() % (2*NUM_SLOTS);
        int bitmapindex;

        evictentry = NULL;
        
        if(index >= 0 && index <NUM_SLOTS){
            bitmapindex = h1 * NUM_SLOTS + index;
            if(cuckoohashtable->keys_accessed_bitmap[bitmapindex] == 0){
                cuckoohashtable->keys_accessed_bitmap[bitmapindex] = 1;
                eviction_path[evicted_node_counter++] = bitmapindex;
                evictentry = first[index];
            }  
        }
        else{
            bitmapindex = h2 * NUM_SLOTS + (index - NUM_SLOTS);
            if(cuckoohashtable->keys_accessed_bitmap[bitmapindex] == 0){
                cuckoohashtable->keys_accessed_bitmap[bitmapindex] = 1;
                eviction_path[evicted_node_counter++] = bitmapindex;
                evictentry = second[index - NUM_SLOTS];
            } 
        }

        if(evictentry == NULL){

            for(i=0; i<NUM_SLOTS; i++){
                if(cuckoohashtable->keys_accessed_bitmap[(h1 * NUM_SLOTS) + i] == 0){
                    cuckoohashtable->keys_accessed_bitmap[(h1 * NUM_SLOTS) + i] = 1;
                    eviction_path[evicted_node_counter++] = (h1 * NUM_SLOTS) + i;
                    evictentry = first[i];
                    //TODO EvictedKey 
                }
            }
            for(i=0; i<NUM_SLOTS; i++){
                if(cuckoohashtable->keys_accessed_bitmap[(h2 * NUM_SLOTS) + i] == 0){
                    cuckoohashtable->keys_accessed_bitmap[(h2 * NUM_SLOTS) + i] = 1;
                    eviction_path[evicted_node_counter++] = (h2 * NUM_SLOTS) + i;
                    evictentry = second[i];
                    //TODO EvictedKey 
                }
            }
        }

        if(evictentry == NULL){
            //TODO CYCLE DETECTED. 
        } 

        printf("Evicted Key %s with index %d\n", evictentry.entryNodeptr->key, index);
        strcpy(temp_key, evictentry.entryNodeptr->key);
        strcpy(temp_value, evictentry.entryNodeptr->value);

        //evictentry.tag = tag;
        //strcpy(evictentry.entryNodeptr->key, curr_key);
        //strcpy(evictentry.entryNodeptr->value, curr_value);
        //printHashTable();
        
        strcpy(curr_key, temp_key);
        strcpy(curr_value, temp_value);
    }
    
    entryNode* finalevictedNode = (entryNode *) malloc(sizeof(entryNode));
    finalevictedNode->key = curr_key;
    finalevictedNode->value = curr_value;
    return finalevictedNode;
}

void evictEntriesFromPath(int* 


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

    pthread_mutex_lock(&hashtable->write_lock);
    entryNode* res = _put(cuckoohashtable, key, value);
   
    if(res != NULL){
        printHashTable();
        resize((cuckoohashtable->num_buckets) * 2);
        _put(cuckoohashtable, res->key, res->value);
        printHashTable();
    }
    
    pthread_mutex_unlock(&hashtable->write_lock);
    printf("Put a New value : %s\n", value);
    return;
}


int main(void){
    cuckoohashtable = createHashTable(3);
    printf("createHashTable: num_buckets %d\n", cuckoohashtable->num_buckets);

    char** keys = (char**)malloc(100*sizeof(char*)); 
    printf("allocated keys\n");
    char* value = (char*)malloc(100*sizeof(char)); 
    printf("allocated values\n");
    
    int i;
    for(i=0;i<11;i++){
        keys[i] = (char*)malloc(10*sizeof(char));
        //printf("allocated key %d\n",i);
        snprintf(keys[i],10,"%d",i);
        //printf("key%d: %s\n", i, keys[i]);
        snprintf(value,20,"%d%s",i,"values");
        //printf("value: %s\n",value);
        put(keys[i],value);
        //printf("put in entry %d\n", i);
    }
    
    printf("testing resize\n");
    for(i=11;i<30;i++){
        keys[i] = (char*)malloc(10*sizeof(char));
        snprintf(keys[i],10,"%d",i);
        snprintf(value,20,"%d%s",i,"values");
        put(keys[i],value);
    }
    
    
    printf("Get\n");
    
    printHashTable();
    printf("\n");
    
    for(i=0;i<20;i++){
        printf("key: %s, value: %s\n", keys[i],get(keys[i]));
    }
 
    printf("createHashTable: num_buckets %d\n", cuckoohashtable->num_buckets);

    printHashTable();
    printf("\n\n Remove \n\n");

    for(i=0;i<5;i++){
        printf("remove key %s : %d\n", keys[i], removeKey(keys[i]));
    }
    
    printHashTable();

    return 0;
}