#include "parallelHashTable.h"

int num_entries_per_thread = 0;
#define NUM_TOTAL_ENTRIES 1000000

hashTable* hashtable;

int hash(char* key){
    unsigned long int hashval = 1;
    int i = 0;

    /* Convert string to an integer */
    while( hashval < INT_MAX && i < strlen( key ) ) {
        hashval = hashval << 8;
        hashval += key[i];
        i++;
    }
    return hashval;    
}

void createHashTable(int num_buckets, float max_load_factor){
   
    hashtable = (hashTable*)malloc(1*sizeof(hashTable));
    hashtable->buckets = (bucketNode*)malloc(num_buckets*sizeof(bucketNode));
    int i;

    for(i=0;i<num_buckets;i++){
        hashtable->buckets[i].firstNode = NULL;
        pthread_mutex_init(&(hashtable->buckets[i].bucketLock), NULL);
    }

    hashtable->num_buckets = num_buckets;
    hashtable->num_entries = 0;
    hashtable->max_load_factor = max_load_factor;
    pthread_rwlock_init(&(hashtable->hashTableLock), NULL);				

    return;
}
 
float getLoadFactor(){
   pthread_rwlock_rdlock(&(hashtable->hashTableLock));
   float curr_load_factor = 1.0*hashtable->num_entries/hashtable->num_buckets;
   pthread_rwlock_unlock(&(hashtable->hashTableLock));
   return curr_load_factor;     
}

entryNode* createEntryNode(char* key, char* value){
    entryNode* newEntryNode = (entryNode *) malloc(sizeof(entryNode));
    newEntryNode->key = (char *) malloc(sizeof(char) * MAX_SIZE);
    newEntryNode->value = (char *) malloc(sizeof(char) * MAX_SIZE);
    strcpy(newEntryNode->key,key);
    strcpy(newEntryNode->value,value);
    newEntryNode->next = NULL;
    return newEntryNode;   
}

char* get(char* key){
    int hashIndex = hash(key)%hashtable->num_buckets;
    bucketNode* bucketnode = &(hashtable->buckets[hashIndex]);
    pthread_mutex_lock(&(bucketnode->bucketLock));
    entryNode* temp = bucketnode->firstNode;
    while(temp!=NULL){
        if(!strcmp(temp->key,key)){
            pthread_mutex_unlock(&(bucketnode->bucketLock));        
	    return temp->value;
        }
        temp=temp->next;
    }
    pthread_mutex_unlock(&(bucketnode->bucketLock));        
    return NULL;
}

void reinsertNode(char* key, char* value){
    int hashIndex = hash(key)%hashtable->num_buckets;
    bucketNode* bucketnode = &(hashtable->buckets[hashIndex]);
    entryNode* prev = NULL;
    entryNode* curr = bucketnode->firstNode;
   
    while(curr!=NULL){
        if(!strcmp(curr->key,key)){
            strcpy(curr->value,value);
	    return;
        }
        prev=curr;
        curr=curr->next;
    }
    hashtable->num_entries++;
    entryNode* temp = createEntryNode(key,value);
    if(prev==NULL){
        bucketnode->firstNode=temp;
    }
    else{
        prev->next=temp;
    }
}


void resize() {
    pthread_rwlock_wrlock(&(hashtable->hashTableLock));
    if((1.0*hashtable->num_entries/hashtable->num_buckets) < hashtable->max_load_factor){
        pthread_rwlock_unlock(&(hashtable->hashTableLock));
        return;
    }
    
    bucketNode* old_buckets = hashtable->buckets;
    int old_num_buckets = hashtable->num_buckets;
    hashtable->num_buckets *= 2;
    hashtable->num_entries = 0;
    hashtable->buckets = (bucketNode*)malloc(hashtable->num_buckets*sizeof(bucketNode));
    
    int i;
    for(i=0; i<hashtable->num_buckets;i++){
        hashtable->buckets[i].firstNode=NULL;
        pthread_mutex_init(&(hashtable->buckets[i].bucketLock), NULL);
    }

    for(i=0;i<old_num_buckets;i++){
        entryNode* curr = old_buckets[i].firstNode;
        entryNode* temp;
        while(curr!=NULL){
            temp=curr;
            reinsertNode(curr->key,curr->value);
            curr=curr->next;
            free(temp);
        }
    }
    pthread_rwlock_unlock(&(hashtable->hashTableLock));
    free(old_buckets);
    return;
}


void put(char* key, char* value){
    pthread_rwlock_rdlock(&(hashtable->hashTableLock));
    int hashIndex = hash(key)%hashtable->num_buckets;
    bucketNode* bucketnode = &(hashtable->buckets[hashIndex]);
    pthread_mutex_lock(&(bucketnode->bucketLock));
    entryNode* prev = NULL;
    entryNode* curr = bucketnode->firstNode;
    while(curr!=NULL){
        if(!strcmp(curr->key,key)){
            strcpy(curr->value,value);
            pthread_mutex_unlock(&(bucketnode->bucketLock));        
            pthread_rwlock_unlock(&(hashtable->hashTableLock));
	    return;
        }
        prev=curr;
        curr=curr->next;
    }
    hashtable->num_entries++;
    entryNode* temp = createEntryNode(key,value);
    if(prev==NULL){
        bucketnode->firstNode=temp;
    }
    else{
        prev->next=temp;
    }
    pthread_mutex_unlock(&(bucketnode->bucketLock));        
    pthread_rwlock_unlock(&(hashtable->hashTableLock));
    if (getLoadFactor() > hashtable->max_load_factor){
	resize();
    }
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
    printf("Parallel Hash Map:\n");

    int num_threads = atoi(argc[1]);
    int num_buckets = atoi(argc[2]);

    createHashTable(num_buckets,1.5);

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

    printf("PUT: time taken: %.9lf\n", end_time - start_time);
    
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
    
    printf("GET: time taken: %.9lf\n", end_time - start_time);
    
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
      
    printf("GETPUT: time taken: %.9lf\n", end_time - start_time);
    
    return 0;
}
