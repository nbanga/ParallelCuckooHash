#include "parallelCuckooHashMap.h"

int num_entries_per_thread=0;
cuckooHashTable* cuckoohashtable;

cuckooHashTable* createHashTable(int num_buckets){

    cuckooHashTable* hashtable = (cuckooHashTable *) malloc(sizeof(cuckooHashTable));
    hashtable->buckets = (bucketNode *) malloc(num_buckets * sizeof(bucketNode));

    int i = 0, j = 0;
    for(; i<num_buckets; i++){
        hashtable->buckets[i].firstNode = (entryNode*)malloc(NUM_SLOTS * sizeof(entryNode));
        pthread_mutex_init(&(hashtable->buckets[i].bucketLock),NULL);
        for(j = 0; j<NUM_SLOTS; j++){
            hashtable->buckets[i].firstNode[j].key = NULL;
            hashtable->buckets[i].firstNode[j].value = NULL;
        }
    }

    hashtable->num_buckets = num_buckets;
    pthread_rwlock_init(&(hashtable->hashTableLock),NULL);
    return hashtable;
}

void computehash(char* key, uint32_t *h1, uint32_t *h2){
    hashlittle2((void *)key, strlen(key), h1, h2);
}

void printHashTable(void){
    int buckets = cuckoohashtable->num_buckets;
    int i,j;
    bucketNode *newNode;

    for(i=0; i<buckets; i++){
         newNode = &(cuckoohashtable->buckets[i]);
         for(j=0; j<NUM_SLOTS;j++){
             if(newNode->firstNode[j].key != NULL){
                 printf("%s ",newNode->firstNode[j].key);
             }
         }
    }
    printf("\n\n");
}

char* get(char* key){
    uint32_t h1 = 0, h2 = 0;
    entryNode* temp;
    int i;    
    computehash(key, &h1, &h2);
    h1 = h1 % cuckoohashtable->num_buckets;
    pthread_mutex_lock(&(cuckoohashtable->buckets[h1].bucketLock));
    temp = cuckoohashtable->buckets[h1].firstNode;    
    for(i = 0; i< NUM_SLOTS; i++){
       if(temp[i].key != NULL && !strcmp(temp[i].key, key)){
           pthread_mutex_unlock(&(cuckoohashtable->buckets[h1].bucketLock));
           return temp[i].value;
       }
    } 
    pthread_mutex_unlock(&(cuckoohashtable->buckets[h1].bucketLock));
    
    if(h1!=h2){
        h2 = h2 % cuckoohashtable->num_buckets;
        pthread_mutex_lock(&(cuckoohashtable->buckets[h2].bucketLock));
        temp = cuckoohashtable->buckets[h2].firstNode;    
        for(i = 0; i< NUM_SLOTS; i++){
           if(temp[i].key != NULL && !strcmp(temp[i].key, key)){
              pthread_mutex_unlock(&(cuckoohashtable->buckets[h2].bucketLock));
              return temp[i].value;
           }
        } 
        pthread_mutex_unlock(&(cuckoohashtable->buckets[h2].bucketLock));
    }
    return NULL;
}

bool removeKey(char* key){
    pthread_rwlock_rdlock(&(cuckoohashtable->hashTableLock));
    uint32_t h1 = 0, h2 = 0;
    entryNode* temp;
    int i;    

    computehash(key, &h1, &h2);
    
    h1 = h1 % cuckoohashtable->num_buckets;
    pthread_mutex_lock(&(cuckoohashtable->buckets[h1].bucketLock));
    temp = cuckoohashtable->buckets[h1].firstNode;    
    for(i = 0; i< NUM_SLOTS; i++){
       if(temp[i].key != NULL && !strcmp(temp[i].key, key)){
           free(temp[i].key);
           free(temp[i].value);
           temp[i].key = NULL;
           temp[i].value = NULL;
           pthread_mutex_unlock(&(cuckoohashtable->buckets[h1].bucketLock));
           pthread_rwlock_unlock(&(cuckoohashtable->hashTableLock));
           return true;
       }
    } 

    h2 = h2 % cuckoohashtable->num_buckets;
    pthread_mutex_lock(&(cuckoohashtable->buckets[h2].bucketLock));
    temp = cuckoohashtable->buckets[h2].firstNode;    
    for(i = 0; i< NUM_SLOTS; i++){
       if(temp[i].key != NULL && !strcmp(temp[i].key, key)){
           free(temp[i].key);
           free(temp[i].value);
           temp[i].key = NULL;
           temp[i].value = NULL;
           pthread_mutex_unlock(&(cuckoohashtable->buckets[h2].bucketLock));
           pthread_rwlock_unlock(&(cuckoohashtable->hashTableLock));
           return true;
       }
    }
    
    pthread_mutex_unlock(&(cuckoohashtable->buckets[h1].bucketLock));
    pthread_mutex_unlock(&(cuckoohashtable->buckets[h2].bucketLock));
    pthread_rwlock_unlock(&(cuckoohashtable->hashTableLock));
    return false;
}

/*void freeHashTable(cuckooHashTable* newHashTable){
    int i,j;

    for(i=0; i<newHashTable->num_buckets; i++){
        for(j=0;j<NUM_SLOTS;j++){
            if(newHashTable->buckets[i].firstNode[j].key!=NULL){
                free(newHashTable->buckets[i].firstNode[j].key);
            }
            if(newHashTable->buckets[i].firstNode[j].value!=NULL){
                free(newHashTable->buckets[i].firstNode[j].value);
            }
        }
        free(newHashTable->buckets[i].firstNode);
    }
    free(newHashTable->buckets);
    free(newHashTable);
    return;
}*/

entryNode* _put(char *key, char *value, int thread_id){
    int num_iterations = 0;
    uint32_t h1 = 0, h2 = 0;
    entryNode *first, *second;
    entryNode evictentry;
    unsigned int t = (unsigned)thread_id;
    int i;

    char* temp_key = (char *) malloc(sizeof(char) * MAX_SIZE);
    char* temp_value = (char *) malloc(sizeof(char) * MAX_SIZE);
    char* curr_key = (char *) malloc(sizeof(char) * MAX_SIZE);
    char* curr_value = (char *) malloc(sizeof(char) * MAX_SIZE);
    strcpy(curr_key, key);
    strcpy(curr_value,value);

    srand48((unsigned)t);
    while(num_iterations < MAX_ITERATIONS){
        num_iterations++;
        computehash(curr_key, &h1, &h2);
        h1 = h1 % cuckoohashtable->num_buckets;
        h2 = h2 % cuckoohashtable->num_buckets;
        if(h1>h2){
            uint32_t temp=h1; 
            h1=h2;
            h2=temp;
        }
        first = cuckoohashtable->buckets[h1].firstNode;    
        second = cuckoohashtable->buckets[h2].firstNode;
        pthread_mutex_lock(&(cuckoohashtable->buckets[h1].bucketLock));
        for(i = 0; i< NUM_SLOTS; i++){
            if(first[i].key == NULL){
                first[i].key = (char *) malloc(sizeof(char) * MAX_SIZE);
                first[i].value = (char *) malloc(sizeof(char) * MAX_SIZE);
                strcpy(first[i].key, curr_key);
                strcpy(first[i].value, curr_value);
                pthread_mutex_unlock(&(cuckoohashtable->buckets[h1].bucketLock));
                return NULL;
            }
            else if(!strcmp(first[i].key,curr_key)){
                strcpy(first[i].value, curr_value);
                pthread_mutex_unlock(&(cuckoohashtable->buckets[h1].bucketLock));
                return NULL;
            }
        }
       
        if(h1!=h2){
            pthread_mutex_lock(&(cuckoohashtable->buckets[h2].bucketLock));
            for(i = 0; i< NUM_SLOTS; i++){
                if(second[i].key == NULL){
                    second[i].key = (char *) malloc(sizeof(char) * MAX_SIZE);
                    second[i].value = (char *) malloc(sizeof(char) * MAX_SIZE);
                    strcpy(second[i].key, curr_key);
                    strcpy(second[i].value, curr_value);
                    pthread_mutex_unlock(&(cuckoohashtable->buckets[h1].bucketLock));
                    pthread_mutex_unlock(&(cuckoohashtable->buckets[h2].bucketLock));
                    return NULL;
                }
                else if(!strcmp(second[i].key,curr_key)){
                    strcpy(second[i].value, curr_value);
                    pthread_mutex_unlock(&(cuckoohashtable->buckets[h1].bucketLock));
                    pthread_mutex_unlock(&(cuckoohashtable->buckets[h2].bucketLock));
                    return NULL;
                }
            }
        }

        int index = rand_r(&t) % (2*NUM_SLOTS);
        if(index >= 0 && index <NUM_SLOTS){
            evictentry = first[index];
        }
        else{
            evictentry = second[index - NUM_SLOTS];
        }

        strcpy(temp_key, evictentry.key);
        strcpy(temp_value, evictentry.value);
        strcpy(evictentry.key, curr_key);
        strcpy(evictentry.value, curr_value);
        strcpy(curr_key, temp_key);
        strcpy(curr_value, temp_value);
        pthread_mutex_unlock(&(cuckoohashtable->buckets[h1].bucketLock));
        if(h1!=h2){
            pthread_mutex_unlock(&(cuckoohashtable->buckets[h2].bucketLock));
        }
    }
    entryNode* finalevictedNode = (entryNode *) malloc(sizeof(entryNode));
    finalevictedNode->key = curr_key;
    finalevictedNode->value = curr_value;
    return finalevictedNode;
}

void resize(int req_buckets, int thread_id){
    pthread_rwlock_wrlock(&(cuckoohashtable->hashTableLock)); 
    if(req_buckets<=cuckoohashtable->num_buckets){
        pthread_rwlock_unlock(&(cuckoohashtable->hashTableLock)); 
        return;
    }
    bucketNode* oldBuckets = cuckoohashtable->buckets;
    int old_num_buckets = cuckoohashtable->num_buckets;
    int i, j;
    entryNode *res, *bucketRow;
    while(true){
        cuckoohashtable->num_buckets*=2;
        cuckoohashtable->buckets = (bucketNode*)malloc(cuckoohashtable->num_buckets*sizeof(bucketNode));

        for(i=0;i<cuckoohashtable->num_buckets;i++){
            cuckoohashtable->buckets[i].firstNode = (entryNode*)malloc(NUM_SLOTS*sizeof(entryNode));
            pthread_mutex_init(&(cuckoohashtable->buckets[i].bucketLock),NULL);
            for(j=0;j<NUM_SLOTS;j++){
                cuckoohashtable->buckets[i].firstNode[j].key=NULL;
                cuckoohashtable->buckets[i].firstNode[j].value=NULL;
            }
        }

        for(i = 0; i < old_num_buckets; i++){
            bucketRow = oldBuckets[i].firstNode; 
            for(j=0; j < NUM_SLOTS; j++){
                if(bucketRow[j].key != NULL){
                    res = _put(bucketRow[j].key, bucketRow[j].value, thread_id);
                    if(res != NULL){
                        break;
                    }
                }
            }
            if(res != NULL)
                break;        
        }
        if(res != NULL){
            bucketNode* temp = cuckoohashtable->buckets;
            free(temp);
        }
        else{
            break;
        }
    }
    pthread_rwlock_unlock(&(cuckoohashtable->hashTableLock)); 
    free(oldBuckets);
}

void put(char* key, char* value, int thread_id){
    pthread_rwlock_rdlock(&(cuckoohashtable->hashTableLock)); 
    entryNode* res = _put(key, value, thread_id);
   
    if(res != NULL){
        int old_num_buckets = cuckoohashtable->num_buckets;
        pthread_rwlock_unlock(&(cuckoohashtable->hashTableLock)); 
        resize(old_num_buckets*2, thread_id);
        pthread_rwlock_rdlock(&(cuckoohashtable->hashTableLock)); 
        _put(res->key, res->value, thread_id);
    }
    pthread_rwlock_unlock(&(cuckoohashtable->hashTableLock)); 
    return;
}

void *putthreadfunc(void *id){
    int thread_id = *(int *)id;
    char* keys = (char*)malloc(20*sizeof(char));  
    int i;
    for(i=0;i<num_entries_per_thread;i++){
        snprintf(keys,20,"%d",i + (100000*thread_id));
        put(keys,keys, thread_id);
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
            put(keys,keys,thread_id);
            count=0;
        }
        else{
            count++;
            snprintf(keys,20,"%d",i + (1000*thread_id));
            get(keys);
        }
    }
    return;
}
 

int main(int argv, char** argc){

    int num_threads = atoi(argc[1]);
    int num_buckets = TOTAL_BUCKETS;

    printf("\nParallel Cuckoo Hashmap : %d threads\n", num_threads);
    cuckoohashtable = createHashTable(num_buckets);

    num_entries_per_thread = (int) TOTAL_ENTRIES/num_threads;

    pthread_t putthreads[num_threads];
    pthread_t getthreads[num_threads];
    pthread_t getputthreads[num_threads];
    
    struct timeval init, end;

    double start_time, end_time;
    printf("Preloading data\n");
    
    int count = 0;
    for(count = 0; count < num_threads; count ++){
    	pthread_create(&putthreads[count], NULL, putthreadfunc, (void *) &count); 
    }
    
    for(count = 0; count < num_threads; count ++){
	pthread_join(putthreads[count], NULL);
    }
    
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

