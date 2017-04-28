#include "lockFreeReadTaggedCuckooHashMap.h"
#define NUM_TOTAL_ENTRIES 1000000

cuckooHashTable* cuckoohashtable;
int num_entries_per_thread;

cuckooHashTable* createHashTable(int num_buckets){

    //printf("Create HASHTABLE Started\n");
    cuckooHashTable* hashtable = (cuckooHashTable *) malloc(sizeof(cuckooHashTable));
    hashtable->buckets = (tagNode **) malloc(num_buckets * sizeof(tagNode *));
    hashtable->keys_accessed_bitmap = (int *) malloc(sizeof(int) * num_buckets * NUM_SLOTS);
    
    int i = 0, j = 0;
    for(; i<num_buckets; i++){
        hashtable->buckets[i] = (tagNode *) malloc (NUM_SLOTS * sizeof(tagNode));

        for(j = 0; j<NUM_SLOTS; j++){
            hashtable->buckets[i][j].entryNodeptr = NULL;
            hashtable->keys_accessed_bitmap[i * NUM_SLOTS + j] = 0;
        }
    }

    hashtable->num_buckets = num_buckets;
    hashtable->version_counter = (uint32_t *) malloc(sizeof(uint32_t) * VERSION_COUNTER_SIZE);
    pthread_mutex_init(&hashtable->write_lock, NULL);    
    pthread_rwlock_init(&(hashtable->hashTableLock),NULL);

    //printf("Create HashTABLE complete\n");
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
    //printf("Print Hash Table\n");
    int buckets = cuckoohashtable->num_buckets;
    int i,j;
    tagNode *newNode;
    int total = 0;

    for(i=0; i<buckets; i++){
        //printf("Bucket %d \t", i);
        
         newNode = cuckoohashtable->buckets[i];
         for(j=0; j<NUM_SLOTS;j++){
             if(newNode[j].entryNodeptr != NULL){
                 printf("%d %s; ",newNode[j].tag, newNode[j].entryNodeptr->key);
                 total++;
		 //printf("%s ", newNode[j].entryNodeptr->key);
             }
         }
         //printf("\n");
    }
        //printf("Total : %d\n", total);
}

char* get(char* key){
    uint32_t h1 = 0, h2 = 0;
    unsigned char tag;
    tagNode *first, *second;
    int i, ver_index, flag = 1;
    uint32_t start_ver_val, end_ver_val;    

    computehash(key, &h1, &h2);
    
    tag = hashTag(h1);    
    h1 = h1 % cuckoohashtable->num_buckets;
    h2 = h2 % cuckoohashtable->num_buckets;
    first = cuckoohashtable->buckets[h1];  
    second = cuckoohashtable->buckets[h2];    
    ver_index = (h1 * NUM_SLOTS) % VERSION_COUNTER_SIZE;

    //printf("GET H1: %d, H2: %d, key: %s\n", h1, h2, key);
  
    while(flag == 1){
	flag = 0;
        start_ver_val = __sync_fetch_and_add(&cuckoohashtable->version_counter[ver_index], 0);        
        for(i = 0; i< NUM_SLOTS; i++){
            //printf("Temp1 Tag: %d, My Tag : %d\n", first[i].tag, tag);
            if(first[i].tag == tag && first[i].entryNodeptr!=NULL){
               //printf("Temp Val: %s\n", first[i].entryNodeptr->key);
               entryNode* entrynode = first[i].entryNodeptr;
               if(!strcmp(key,entrynode->key)){
                   char* result = entrynode->value;
                   end_ver_val = __sync_fetch_and_add(&cuckoohashtable->version_counter[ver_index], 0);
                   if(start_ver_val != end_ver_val || (start_ver_val & 0x1)){
                          flag = 1;
			  break;
                   }
                   //printf("Get : Key : %s at [%d][%d]\n", first[i].entryNodeptr->key, h1, i);
		   return result;
               }
           }
        } 
     
	if(flag == 1)
	    continue;

        for(i = 0; i< NUM_SLOTS; i++){
            //printf("Temp1 Tag: %d, My Tag : %d\n", second[i].tag, tag);
            if(second[i].tag == tag && second[i].entryNodeptr!=NULL){
               //printf("Temp Val: %s\n", second[i].entryNodeptr->key);
               entryNode* entrynode = second[i].entryNodeptr;
               if(!strcmp(key,entrynode->key)){
                   char* result = entrynode->value;
                   end_ver_val = __sync_fetch_and_add(&cuckoohashtable->version_counter[ver_index], 0);
                   if(start_ver_val != end_ver_val || (start_ver_val & 0x1)){
                       flag = 1;     
		       break;
                   }
                   //printf("Get : Key : %s at [%d][%d]\n", second[i].entryNodeptr->key, h1, i);
		   return result;
               }
           }
        }    
    }

    return NULL;
}

void evictEntriesFromPath(int* eviction_path, int* evicted_key_version, int eviction_path_counter, int evicted_key_version_counter, char* key, char* value){
    
    //printf("EvictEntriesFromPath, epcounter: %d\n", eviction_path_counter);	
    if(eviction_path_counter == 0){
        int index = eviction_path[0];
        int bucket = index / NUM_SLOTS;
        int slot = index % NUM_SLOTS;
        
        uint32_t h1 = 0, h2 = 0;
        computehash(key, &h1, &h2);
        unsigned char tag = hashTag(h1);    
 
        //printf("evictENtries 0: inserting key %s in bucket,index: %d %d\n", key, eviction_path_counter, bucket, slot);
        entryNode *entrynode = (entryNode *) malloc(sizeof(entryNode));
        entrynode->key = (char*) malloc(MAX_SIZE*sizeof(char));
        entrynode->value = (char*) malloc(MAX_SIZE*sizeof(char));   
        strcpy(entrynode->key, key);
        strcpy(entrynode->value, value);
        cuckoohashtable->buckets[bucket][slot].tag = tag;
        cuckoohashtable->buckets[bucket][slot].entryNodeptr = entrynode;
        //printHashTable();
        fflush(stdout);
        return;
    }
    else{
        int src_index, evicted_key_version_index, i;
        int src_bucket, src_slot;
        //tagNode src_node, dest_node;
        int dest_index = eviction_path[eviction_path_counter];
        int dest_bucket = dest_index / NUM_SLOTS;
        int dest_slot = dest_index % NUM_SLOTS;
        
        cuckoohashtable->keys_accessed_bitmap[dest_index]=0;
        
        for(i=eviction_path_counter-1;i>=0;i--){
            src_index = eviction_path[i];
            evicted_key_version_index = evicted_key_version[--evicted_key_version_counter];
            cuckoohashtable->keys_accessed_bitmap[src_index]=0;
            
            src_bucket = src_index / NUM_SLOTS;
            src_slot = src_index % NUM_SLOTS;
            dest_bucket = dest_index / NUM_SLOTS;
            dest_slot = dest_index % NUM_SLOTS;
           
            __sync_fetch_and_add(&cuckoohashtable->version_counter[evicted_key_version_index],1);
            tagNode* src_node = &(cuckoohashtable->buckets[src_bucket][src_slot]);
            tagNode* dest_node = &(cuckoohashtable->buckets[dest_bucket][dest_slot]);
            dest_node->tag = src_node->tag;
            dest_node->entryNodeptr = (entryNode*) malloc(1*sizeof(entryNode));
            dest_node->entryNodeptr->key = (char*)malloc(sizeof(char)*MAX_SIZE);
            dest_node->entryNodeptr->value = (char*)malloc(sizeof(char)*MAX_SIZE);       
            //printf("evictENtries For loop %d: inserting key %s in bucket,index: %d %d\n", eviction_path_counter, src_node->entryNodeptr->key, dest_bucket, dest_slot);
            strcpy(dest_node->entryNodeptr->key,src_node->entryNodeptr->key);
            strcpy(dest_node->entryNodeptr->value,src_node->entryNodeptr->value);
            if(src_node->entryNodeptr!=NULL){
                if(src_node->entryNodeptr->key!=NULL){
                    free(src_node->entryNodeptr->key);
                }
                 if(src_node->entryNodeptr->value!=NULL){
                    free(src_node->entryNodeptr->value);
                }
                free(src_node->entryNodeptr);
                src_node->entryNodeptr=NULL;
            }
            __sync_fetch_and_add(&cuckoohashtable->version_counter[evicted_key_version_index],1);
            //printHashTable();
            fflush(stdout);
            dest_index = src_index;
        }
        
        int final_index = eviction_path[0];
        dest_bucket = final_index / NUM_SLOTS;
        dest_slot = final_index % NUM_SLOTS;
        uint32_t h1 = 0, h2 = 0;
        computehash(key, &h1, &h2);
        unsigned char tag = hashTag(h1);    
        cuckoohashtable->keys_accessed_bitmap[final_index]=0;
       
        __sync_fetch_and_add(&cuckoohashtable->version_counter[evicted_key_version_index],1);
        tagNode* dest_node = &(cuckoohashtable->buckets[dest_bucket][dest_slot]);
        dest_node->tag = tag;
        dest_node->entryNodeptr = (entryNode*) malloc(1*sizeof(entryNode));
        dest_node->entryNodeptr->key = (char*)malloc(sizeof(char)*MAX_SIZE);
        dest_node->entryNodeptr->value = (char*)malloc(sizeof(char)*MAX_SIZE);
        strcpy(dest_node->entryNodeptr->key,key);
        strcpy(dest_node->entryNodeptr->value,value);
        __sync_fetch_and_add(&cuckoohashtable->version_counter[evicted_key_version_index],1);
        //printf("evictENtries Final: inserting key %s in bucket,index: %d %d\n", key, dest_bucket, dest_slot);
        //printHashTable();
    }        
}

bool _put(char *key, char *value){
    //printf("_put begin\n");
    int num_iterations = 0;
    uint32_t h1 = 0, h2 = 0;
    tagNode *first, *second;
    unsigned char tag;
    int i, ver_index;
    int eviction_path[MAX_ITERATIONS];
    int evicted_key_version[MAX_ITERATIONS];
    int evicted_path_counter = 0;
    int evicted_key_version_counter = 0;
        
    char* curr_key = (char *) malloc(sizeof(char) * MAX_SIZE);
    char* curr_value = (char *) malloc(sizeof(char) * MAX_SIZE);

    strcpy(curr_key, key);
    strcpy(curr_value, value);
    
    while(num_iterations < MAX_ITERATIONS){
        //printf("Num Iterations %d for key %s\n", num_iterations, key);
        num_iterations++;
        h1 = 0; 
        h2 = 0;
        computehash(curr_key, &h1, &h2);
        h1 = h1 % cuckoohashtable->num_buckets;
        h2 = h2 % cuckoohashtable->num_buckets;
        first = cuckoohashtable->buckets[h1];    
        second = cuckoohashtable->buckets[h2];
        ver_index = (h1 * NUM_SLOTS) % VERSION_COUNTER_SIZE;
        
        //printf("PUT H1: %d, H2: %d, key: %s\n", h1, h2, curr_key);

        for(i = 0; i< NUM_SLOTS; i++){
            if (first[i].entryNodeptr == NULL){
               eviction_path[evicted_path_counter] = (h1 * NUM_SLOTS) + i;
               evictEntriesFromPath(eviction_path, evicted_key_version, evicted_path_counter, evicted_key_version_counter, key, value);
               return true;
            }
            else if(num_iterations == 1 && !strcmp(first[i].entryNodeptr->key,curr_key)){
                __sync_fetch_and_add(&cuckoohashtable->version_counter[ver_index], 1);
                strcpy(first[i].entryNodeptr->value,curr_value);
        	//printf("Value1 Updated\n");
		__sync_fetch_and_add(&cuckoohashtable->version_counter[ver_index], 1);
                return true;
            }
        }
        
        for(i = 0; i< NUM_SLOTS; i++){
            if(second[i].entryNodeptr == NULL){
                eviction_path[evicted_path_counter] = (h2 * NUM_SLOTS) + i;
                evictEntriesFromPath(eviction_path, evicted_key_version, evicted_path_counter, evicted_key_version_counter, key, value);
                return true;
            }
            else if(num_iterations == 1 && !strcmp(second[i].entryNodeptr->key,curr_key)){
                __sync_fetch_and_add(&cuckoohashtable->version_counter[ver_index], 1);
                strcpy(second[i].entryNodeptr->value,curr_value);
        	//printf("Value2 Updated\n");
                __sync_fetch_and_add(&cuckoohashtable->version_counter[ver_index], 1);
                return true;
            }
        }

        //int index = rand() % (2*NUM_SLOTS);
        //int bitmapIndex;
        tagNode* evictentry = NULL;
        
        /*if(index >= 0 && index <NUM_SLOTS){
            bitmapindex = h1 * NUM_SLOTS + index;
            if(cuckoohashtable->keys_accessed_bitmap[bitmapindex] == 0){
                cuckoohashtable->keys_accessed_bitmap[bitmapindex] = 1;
                eviction_path[evicted_node_counter++] = bitmapindex;
                evictentry = &first[index];
            }  
        }
        else{
            bitmapindex = h2 * NUM_SLOTS + (index - NUM_SLOTS);
            if(cuckoohashtable->keys_accessed_bitmap[bitmapindex] == 0){
                cuckoohashtable->keys_accessed_bitmap[bitmapindex] = 1;
                eviction_path[evicted_node_counter++] = bitmapindex;
                evictentry = &second[index - NUM_SLOTS];
            } 
        }*/

        //if(evictentry == NULL){

        for(i=0; i<NUM_SLOTS; i++){
            int evict_index = (h1*NUM_SLOTS)+i;
            if(cuckoohashtable->keys_accessed_bitmap[evict_index] == 0){
                cuckoohashtable->keys_accessed_bitmap[evict_index] = 1;
                eviction_path[evicted_path_counter++] = evict_index;
                evictentry = &first[i];
                break;
            }
        }
        if(evictentry==NULL){
            for(i=0; i<NUM_SLOTS; i++){
                int evict_index = (h2*NUM_SLOTS)+i;
                if(cuckoohashtable->keys_accessed_bitmap[evict_index] == 0){
                    cuckoohashtable->keys_accessed_bitmap[evict_index] = 1;
                    eviction_path[evicted_path_counter++] = evict_index;
                    evictentry = &second[i];
                    break;
                }
            }
        }
       // }

        if(evictentry == NULL){
            //printf("cycle detected\n");
            return false;
        } 
        else{
            if(num_iterations != 1){
                evicted_key_version[evicted_key_version_counter++] = ver_index;
            }
        
            //printf("Evicted Key %s with index %d\n", evictentry->entryNodeptr->key,i);
            strcpy(curr_key, evictentry->entryNodeptr->key);
            strcpy(curr_value, evictentry->entryNodeptr->value);

            //evictentry.tag = tag;
            //strcpy(evictentry.entryNodeptr->key, curr_key);
            //strcpy(evictentry.entryNodeptr->value, curr_value);
            
            //strcpy(curr_key, temp_key);
            //strcpy(curr_value, temp_value);
        }
    }

    //printf("num iterations exceeded, resize\n");
    return false;
    
    /*entryNode* finalevictedNode = (entryNode *) malloc(sizeof(entryNode));
    finalevictedNode->key = curr_key;
    finalevictedNode->value = curr_value;
    return finalevictedNode;*/
}

void resize(int thread_id){
    //printf("Resize: num_buckets = %d thread_id = %d\n", cuckoohashtable->num_buckets, thread_id);
    //printf("resize: waiting to lock, thread_id : %d \n", thread_id);
    //printf("resize: acquired lock, thread_id : %d \n", thread_id);
    
    tagNode** oldBuckets = cuckoohashtable->buckets;
    int old_num_buckets = cuckoohashtable->num_buckets;

    bool res;
    int i, j;

    while(true){
	res = true;
	//printf("BUCKETS : %d\n", cuckoohashtable->num_buckets);    
        cuckoohashtable->num_buckets*=2;
	cuckoohashtable->buckets = (tagNode **) malloc(cuckoohashtable->num_buckets * sizeof(tagNode));
	free(cuckoohashtable->keys_accessed_bitmap);
	cuckoohashtable->keys_accessed_bitmap = (int*)malloc(sizeof(int)*cuckoohashtable->num_buckets*NUM_SLOTS);

        for(i=0;i<cuckoohashtable->num_buckets;i++){
            cuckoohashtable->buckets[i] = (tagNode*)malloc(NUM_SLOTS*sizeof(tagNode));
            for(j=0;j<NUM_SLOTS;j++){
                cuckoohashtable->buckets[i][j].entryNodeptr=NULL;
                cuckoohashtable->keys_accessed_bitmap[i * NUM_SLOTS + j] = 0;
            }
        }

        for(i = 0; i < old_num_buckets; i++){ 
            for(j=0; j < NUM_SLOTS; j++){
                if(oldBuckets[i][j].entryNodeptr != NULL){
		    //printf("Resize 2.21 _put called, K:%s, V:%s\n", oldBuckets[i][j].entryNodeptr->key, oldBuckets[i][j].entryNodeptr->value);
                    res = _put(oldBuckets[i][j].entryNodeptr->key, oldBuckets[i][j].entryNodeptr->value);
                    //printf("Resize 2.21 _put returned\n");
		    if(!res){
			break;
                    }
                }
            }
            if(!res){
                break;  
	    }
        }

        if(!res){
            //printf("resize: value evicted left behind: %s\n", res->key);
            //printf("inside resize: free and resize\n");
            //printhashtable();
            tagNode** temp = cuckoohashtable->buckets;
            free(temp);
        }
        else{
            break;
        }
    }
    
    //printhashtable();
    free(oldBuckets);

    //printf("resize: released lock, thread_id : %d \n", thread_id);
}

void put(char* key, char* value, int thread_id){
    // have a gloabl lock on hashtable
    // which is always read mode
    // except in resize where we get write lock on the table
    // printf("put: req wrlock thread %d\n", thread_id);
    pthread_mutex_lock(&cuckoohashtable->write_lock);
    // printf("put: acquired wrlock\n");
    bool result = _put(key, value);
    // printf("put : _put returned %d\n", result);    

    if(!result){
        resize(thread_id);
        _put(key, value);
    }
    
    pthread_mutex_unlock(&cuckoohashtable->write_lock);
    return;
}

void *putthreadfunc(void *id){
    int thread_id = *(int *)id;
    char* keys = (char*)malloc(20*sizeof(char));  
    int i;
    for(i=0;i<num_entries_per_thread;i++){
        snprintf(keys,20,"%d",i + (100000*thread_id));
        put(keys,keys,thread_id);
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
	//printf("GET key: %s, thread: %d\n", keys, thread_id);
    }
    return;
}

void *getputthreadfunc(void* id){
    int thread_id = *(int *)id;
    char* keys = (char*)malloc(20*sizeof(char));  
    int i;
    int count = 0;
    for(i=0;i<num_entries_per_thread;i++){
        if(count==30){
            snprintf(keys,20,"%d",i + (100000*thread_id));
            put(keys,keys,thread_id);
            count = 0;
        }
        else{
            snprintf(keys,20,"%d",i + (1000*thread_id));
            count++;
            get(keys);
        }
	//printf("GET key: %s, thread: %d\n", keys, thread_id);
    }
    return;
}

int main(int argv, char** argc){

    printf("Lock Free Read with Tags Cuckoo Hashmap\n");
    int num_threads = atoi(argc[1]);
    int num_buckets = atoi(argc[2]);

    cuckoohashtable = createHashTable(num_buckets);

    num_entries_per_thread = (int)NUM_TOTAL_ENTRIES/num_threads;

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

    printf("PUT: time taken - %.9lf\n", end_time - start_time);
    
    gettimeofday(&init, NULL);
    start_time = (double)init.tv_sec + (double) init.tv_usec / 1000000.0;
            
    for(count = 0; count < num_threads; count ++){ 
	pthread_create(&getthreads[count], NULL, getthreadfunc, (void *) &count); 
        //printf("get thread %d created\n",count);
    }

    for(count = 0; count < num_threads; count ++){
	pthread_join(getthreads[count], NULL);
        //printf("get thread %d joined\n",count);
    }
    
    gettimeofday(&init, NULL);
    end_time = (double)init.tv_sec + (double) init.tv_usec / 1000000.0;
    printf("GET: time taken -  %.9lf\n", end_time - start_time);
    
    gettimeofday(&init, NULL);
    start_time = (double)init.tv_sec + (double) init.tv_usec / 1000000.0;
    
    for(count = 0; count < num_threads; count ++){
    	pthread_create(&getputthreads[count], NULL, getputthreadfunc, (void *) &count); 
        //printf("getput thread %d created\n",count);
    }
    
    for(count = 0; count < num_threads; count ++){
	pthread_join(getputthreads[count], NULL);
        //printf("getput thread %d joined\n",count);
    }
    
    gettimeofday(&init, NULL);
    end_time = (double)init.tv_sec + (double) init.tv_usec / 1000000.0;
      
    printf("GETPUT: time taken -  %.9lf\n", end_time - start_time);
    
    return 0;
}
