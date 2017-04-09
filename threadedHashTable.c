#include "threadedHashTable.h"

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

bool removeKey(char* key){
    
    pthread_rwlock_rdlock(&(hashtable->hashTableLock));
    int hashIndex = hash(key)%hashtable->num_buckets;
    bucketNode* bucketnode = &(hashtable->buckets[hashIndex]);

    pthread_mutex_lock(&(bucketnode->bucketLock));

    entryNode* prev = NULL;
    entryNode* curr = bucketnode->firstNode;
        
    while(curr!=NULL){
        if(!strcmp(curr->key,key)){
            if(prev==NULL){
                hashtable->buckets[hashIndex].firstNode=NULL;
            }
            else{
                prev->next = curr->next;
            }
            hashtable->num_entries--;
            free(curr);
            pthread_mutex_unlock(&(bucketnode->bucketLock));        
            pthread_rwlock_unlock(&(hashtable->hashTableLock));
  	    return true;
        }
        prev=curr;
        curr=curr->next;
    }
    pthread_mutex_unlock(&(bucketnode->bucketLock));        
    pthread_rwlock_unlock(&(hashtable->hashTableLock));
    return false;
}

char* get(char* key){
    pthread_rwlock_rdlock(&(hashtable->hashTableLock));
 
    int hashIndex = hash(key)%hashtable->num_buckets;
    bucketNode* bucketnode = &(hashtable->buckets[hashIndex]);
    
    pthread_mutex_lock(&(bucketnode->bucketLock));
    entryNode* temp = bucketnode->firstNode;

    while(temp!=NULL){
        if(!strcmp(temp->key,key)){
            pthread_mutex_unlock(&(bucketnode->bucketLock));        
            pthread_rwlock_unlock(&(hashtable->hashTableLock));
	    return temp->value;
        }
        temp=temp->next;
    }

    pthread_mutex_unlock(&(bucketnode->bucketLock));        
    pthread_rwlock_unlock(&(hashtable->hashTableLock));
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
    printf("Resize requesting Write Lock\n");
    pthread_rwlock_wrlock(&(hashtable->hashTableLock));
    printf("Resize acquired Write Lock\n");
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
    printf("Step 3\n");
    for(i=0; i<hashtable->num_buckets;i++){
        hashtable->buckets[i].firstNode=NULL;
        pthread_mutex_init(&(hashtable->buckets[i].bucketLock), NULL);
    }
    printf("Step 4\n");

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
    printf("Step 5\n");
    pthread_rwlock_unlock(&(hashtable->hashTableLock));
    printf("Sep 6\n");
    free(old_buckets);
    printf("RESIZING!!!!!!!!!!!!\n");
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
        printf("Resize Called\n");
	resize();
    }
}

void *putthreadfunc(void *id){
    int thread_id = *(int *)id;

    char* keys = (char*)malloc(20*sizeof(char)); 
    char* value = (char*)malloc(20*sizeof(char)); 
 
    int i;
    for(i=0;i<30;i++){
        //keys[i] = (char*)malloc(10*sizeof(char));
        //printf("allocated key %d\n",i);
        snprintf(keys,10,"%d",i + (i*thread_id));
        //printf("key%d: %s\n", i, keys[i]);
        snprintf(value,20,"%d%s",i,"values");
        //printf("value: %s\n",value);
        put(keys,value);
        printf("put in entry %s by %d\n", keys, thread_id);
    }
    
    return;
}

void *getthreadfunc(void *id){ 
    int thread_id = *(int *)id;
    char* keys = (char*)malloc(20*sizeof(char)); 
 
    int i;
    for(i=0;i<20;i++){
        snprintf(keys,10,"%d",i + (i*thread_id));
	printf("GET key: %s, value: %s, thread: %d\n", keys,get(keys), thread_id);
    }
    return;
}

void *removethreadfunc(void *id){
    int thread_id = 1;
    char* keys = (char*)malloc(20*sizeof(char)); 
 
    int i;
    for(i=0;i<10;i++){
        snprintf(keys,10,"%d",i + (i*thread_id));
	printf("REM key: %s, value: %d, thread: %d\n", keys, removeKey(keys), thread_id);
    }
    return;
}


int main(void){
    createHashTable(2,1.5);
    printf("createHashTable: num_buckets %d\n", hashtable->num_buckets);

    pthread_t putthreads[5];
    pthread_t getthreads[5];
    pthread_t removethreads;

    int count = 0;
    for(count = 0; count < 5; count ++){
    	pthread_create(&putthreads[count], NULL, putthreadfunc, (void *) &count); 
    }
   
    for(count = 0; count < 5; count ++){ 
	pthread_create(&getthreads[count], NULL, getthreadfunc, (void *) &count); 
    }

    pthread_create(&removethreads, NULL, removethreadfunc, (void *) NULL);
  	 
    for(count = 0; count < 1; count ++){
	pthread_join(putthreads[count], NULL);
	pthread_join(getthreads[count], NULL);
    }
    pthread_join(removethreads, NULL);

    printf("Program Completed\n");
    return 0;
}
