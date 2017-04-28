#include "parallelTagCuckooHashMap.h"

int num_entries_per_thread=0;
cuckooHashTable* cuckoohashtable;

cuckooHashTable* createHashTable(int num_buckets){

    cuckooHashTable* hashtable = (cuckooHashTable *) malloc(sizeof(cuckooHashTable));
    hashtable->buckets = (tagNode **) malloc(num_buckets * sizeof(tagNode *));
    hashtable->bucketLocks = (pthread_mutex_t*) malloc(num_buckets * sizeof(pthread_mutex_t));

    int i = 0, j = 0;
    for(; i<num_buckets; i++){
        hashtable->buckets[i] = (tagNode *) malloc (NUM_SLOTS * sizeof(tagNode));
        for(j = 0; j<NUM_SLOTS; j++){
            hashtable->buckets[i][j].entryNodeptr = NULL;
        }
        pthread_mutex_init(&hashtable->bucketLocks[i],NULL);
    }

    hashtable->num_buckets = num_buckets;
    pthread_rwlock_init(&(hashtable->hashTableLock),NULL);
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
    int i;    

    computehash(key, &h1, &h2);
    tag = hashTag(h1);    
    h1 = h1 % cuckoohashtable->num_buckets;
    first = cuckoohashtable->buckets[h1];    
    pthread_mutex_lock(&(cuckoohashtable->bucketLocks[h1]));
    for(i = 0; i< NUM_SLOTS; i++){   
        if(first[i].tag == tag && first[i].entryNodeptr!=NULL){
           entryNode* entrynode = first[i].entryNodeptr;
           if(!strcmp(key,entrynode->key)){
               pthread_mutex_unlock(&(cuckoohashtable->bucketLocks[h1]));
               return entrynode->value;
           }
       }
    } 
    pthread_mutex_unlock(&(cuckoohashtable->bucketLocks[h1]));

    if(h1!=h2){
       h2 = h2 % cuckoohashtable->num_buckets;
       second = cuckoohashtable->buckets[h2];    
       pthread_mutex_lock(&(cuckoohashtable->bucketLocks[h2]));
       for(i = 0; i< NUM_SLOTS; i++){
           if(second[i].tag == tag && second[i].entryNodeptr!=NULL){
               entryNode* entrynode = second[i].entryNodeptr;
               if(!strcmp(key,entrynode->key)){
                   pthread_mutex_unlock(&(cuckoohashtable->bucketLocks[h2]));
                   return entrynode->value;
               }
           }
        }
        pthread_mutex_unlock(&(cuckoohashtable->bucketLocks[h2]));
    }
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
    int i;

    curr_key = key;
    curr_value = value;
    char* temp_key = (char *) malloc(sizeof(char) * MAX_SIZE);
    char* temp_value = (char *) malloc(sizeof(char) * MAX_SIZE);
    
    while(num_iterations < MAX_ITERATIONS){
        //printf("Num Iterations %d for key %s\n", num_iterations, key);
        num_iterations++;
        h1 = 0; h2 = 0;
        computehash(curr_key, &h1, &h2);

        tag = hashTag(h1);    
        h1 = h1 % htptr->num_buckets;
        h2 = h2 % htptr->num_buckets;
        if(h1>h2){
            uint32_t temp=h1; 
            h1=h2;
            h2=temp;
        }
        first = htptr->buckets[h1];    
        second = htptr->buckets[h2];

        
        //printf("PUT H1: %d, H2: %d, key: %s\n", h1, h2, curr_key);

        pthread_mutex_lock(&(cuckoohashtable->bucketLocks[h1]));
        for(i = 0; i< NUM_SLOTS; i++){
            if (first[i].entryNodeptr == NULL){
                entryNode* entrynode = (entryNode*) malloc(1*sizeof(entryNode));
                entrynode->key = (char*) malloc(MAX_SIZE*sizeof(char));
                entrynode->value = (char*) malloc(MAX_SIZE*sizeof(char));
                strcpy(entrynode->key,curr_key);
                strcpy(entrynode->value,curr_value);
                first[i].tag = tag;
                first[i].entryNodeptr = entrynode;
                //printf("First: Inserted Key %s in bucket %d, %d with tag %d\n", key, h1, i, tag); 
                pthread_mutex_unlock(&(cuckoohashtable->bucketLocks[h1]));
                return NULL;
            }
            else if(!strcmp(first[i].entryNodeptr->key,curr_key)){
                strcpy(first[i].entryNodeptr->value,curr_value);
                pthread_mutex_unlock(&(cuckoohashtable->bucketLocks[h1]));
                return NULL;
            }
        }
        
        if(h1!=h2){
            pthread_mutex_lock(&(cuckoohashtable->bucketLocks[h2]));
            for(i = 0; i< NUM_SLOTS; i++){
                if (second[i].entryNodeptr == NULL){
                    entryNode* entrynode = (entryNode*) malloc(1*sizeof(entryNode));
                    entrynode->key = (char*) malloc(MAX_SIZE*sizeof(char));
                    entrynode->value = (char*) malloc(MAX_SIZE*sizeof(char));
                    strcpy(entrynode->key,curr_key);
                    strcpy(entrynode->value,curr_value);
                    second[i].tag = tag;
                    second[i].entryNodeptr = entrynode;
                    //printf("Second: Inserted Key %s in bucket %d, %d with tag %d\n", key, h2, i, tag); 
                    pthread_mutex_unlock(&(cuckoohashtable->bucketLocks[h1]));
                    pthread_mutex_unlock(&(cuckoohashtable->bucketLocks[h2]));
                    return NULL;
                }
                else if(!strcmp(second[i].entryNodeptr->key,curr_key)){
                    strcpy(second[i].entryNodeptr->value,curr_value);
                    pthread_mutex_unlock(&(cuckoohashtable->bucketLocks[h1]));
                    pthread_mutex_unlock(&(cuckoohashtable->bucketLocks[h2]));
                    return NULL;
                }
            }
        }

        int index = rand() % (2*NUM_SLOTS);
        if(index >= 0 && index <NUM_SLOTS){
            evictentry = first[index];
        }
        else{
            evictentry = second[index - NUM_SLOTS];
        }

        //printf("Evicted Key %s with index %d\n", evictentry.entryNodeptr->key, index);
        strcpy(temp_key, evictentry.entryNodeptr->key);
        strcpy(temp_value, evictentry.entryNodeptr->value);

        evictentry.tag = tag;
        strcpy(evictentry.entryNodeptr->key, curr_key);
        strcpy(evictentry.entryNodeptr->value, curr_value);
        //printHashTable();
        
        strcpy(curr_key, temp_key);
        strcpy(curr_value, temp_value);
        
        pthread_mutex_unlock(&(cuckoohashtable->bucketLocks[h1]));
        if(h1!=h2){
            pthread_mutex_unlock(&(cuckoohashtable->bucketLocks[h2]));
        }
                
    }
    
    entryNode* finalevictedNode = (entryNode *) malloc(sizeof(entryNode));
    finalevictedNode->key = curr_key;
    finalevictedNode->value = curr_value;
    return finalevictedNode;
}

void resize(int num_buckets){
    pthread_rwlock_wrlock(&(cuckoohashtable->hashTableLock)); 
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
        freeHashTable(newHashTable);
        resize(2 * num_buckets);
        _put(cuckoohashtable, res->key, res->value);
    }
    freeHashTable(cuckoohashtable);
    cuckoohashtable = newHashTable;
    pthread_rwlock_unlock(&(cuckoohashtable->hashTableLock)); 
}

void put(char* key, char* value){
    pthread_rwlock_rdlock(&(cuckoohashtable->hashTableLock)); 
    entryNode* res = _put(cuckoohashtable, key, value);
   
    if(res != NULL){
        //printHashTable();
        pthread_rwlock_unlock(&(cuckoohashtable->hashTableLock)); 
        resize((cuckoohashtable->num_buckets) * 2);
        pthread_rwlock_rdlock(&(cuckoohashtable->hashTableLock)); 
        _put(cuckoohashtable, res->key, res->value);
        //printHashTable();
    }
    
    pthread_rwlock_unlock(&(cuckoohashtable->hashTableLock)); 
    //printf("Put a New value : %s\n", value);
    return;
}

void *putthreadfunc(void *id){
    int thread_id = *(int *)id;
    char* keys = (char*)malloc(20*sizeof(char));  
    int i;
    for(i=0;i<num_entries_per_thread;i++){
        snprintf(keys,20,"%d",i + (100000*thread_id));
        put(keys,keys);
        //printf("put in entry %s by %d\n", keys, thread_id);
    }    
    return;
}

void *getthreadfunc(void *id){ 
    int thread_id = *(int *)id;
    char* keys = (char*)malloc(20*sizeof(char));  
    int i;
    for(i=0;i<num_entries_per_thread;i++){
        snprintf(keys,20,"%d",i + (1000*thread_id));
        get(keys);
	//printf("GET key: %s, value: %s, thread: %d\n", keys,get(keys), thread_id);
    }
    return;
}

void *getputthreadfunc(void* id){
    int thread_id = *(int *)id;
    char* keys = (char*)malloc(20*sizeof(char));  
    int i;
    int count = 0;
    for(i=0;i<num_entries_per_thread;i++){
        if(count==25){
            snprintf(keys,20,"%d",i + (100000*thread_id));
            put(keys,keys);
            count=0;
        }
        else{
            count++;
            snprintf(keys,20,"%d",i + (1000*thread_id));
            get(keys);
        }
	//printf("GET key: %s, value: %s, thread: %d\n", keys,get(keys), thread_id);
    }
    return;
}
 

int main(int argv, char** argc){

    printf("Parallel Cuckoo Hashmap with Tags\n");
    int num_threads = atoi(argc[1]);
    int num_buckets = TOTAL_BUCKETS;

    cuckoohashtable = createHashTable(num_buckets);

    num_entries_per_thread = (int) TOTAL_ENTRIES/num_threads;
    pthread_t putthreads[num_threads];
    pthread_t getthreads[num_threads];
    pthread_t getputthreads[num_threads];
    
    struct timeval init, end;

    double start_time, end_time;
    gettimeofday(&init, NULL);
    start_time = (double)init.tv_sec + (double) init.tv_usec / 1000000.0;
    
    int count = 0;
    for(count = 0; count < num_threads; count ++){
    	pthread_create(&putthreads[count], NULL, putthreadfunc, (void *) &count); 
    }
    
    for(count = 0; count < num_threads; count ++){
	pthread_join(putthreads[count], NULL);
    }
    
    gettimeofday(&init, NULL);
    end_time = (double)init.tv_sec + (double) init.tv_usec / 1000000.0;

    printf("PUT: time taken -  %.9lf \n", end_time - start_time);
    
    gettimeofday(&init, NULL);
    start_time = (double)init.tv_sec + (double) init.tv_usec / 1000000.0;
            
    for(count = 0; count < num_threads; count ++){ 
	pthread_create(&getthreads[count], NULL, getthreadfunc, (void *) &count); 
    }

    for(count = 0; count < num_threads; count ++){
	pthread_join(getthreads[count], NULL);
    }
    
    gettimeofday(&init, NULL);
    end_time = (double)init.tv_sec + (double) init.tv_usec / 1000000.0;
    
    printf("GET: time taken -  %.9lf\n", end_time - start_time);
    
    gettimeofday(&init, NULL);
    start_time = (double)init.tv_sec + (double) init.tv_usec / 1000000.0;
    
    for(count = 0; count < num_threads; count ++){
    	pthread_create(&getputthreads[count], NULL, getputthreadfunc, (void *) &count); 
    }
    
    for(count = 0; count < num_threads; count ++){
	pthread_join(getputthreads[count], NULL);
    }
 
    gettimeofday(&init, NULL);
    end_time = (double)init.tv_sec + (double) init.tv_usec / 1000000.0;
    
        
    printf("GETPUT: time taken -  %.9lf\n", end_time - start_time);
    return 0;
}


