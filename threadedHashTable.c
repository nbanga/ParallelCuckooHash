#include "threadedHashTable.h"

hashTable* hashtable;

int hash(char* key){
    unsigned long int hashval;
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
    hashtable->buckets = (bucketNode**)malloc(num_buckets*sizeof(bucketNode*));

    int i;

    for(i=0;i<num_buckets;i++){
        hashtable->buckets[i]->entryNode = NULL;
        pthread_mutex_init(&(hashtable->buckets[i]->bucketLock), NULL);
    }

    hashtable->num_buckets = num_buckets;
    hashtable->num_entries = 0;
    hashtable->max_load_factor = max_load_factor;

    return;
}
 
float getLoadFactor(){
    return 1.0*hashtable->num_entries/hashtable->num_buckets;
}

entryNode* createEntryNode(char* key, char* value){
    entryNode* newEntryNode = (entryNode *) malloc(sizeof(entryNode));
    strcpy(newEntryNode->key,key);
    strcpy(newEntryNode->value,value);
    newEntryNode->next = NULL;
    return newEntryNode;   
}

bool removeKey(char* key){
    int hashIndex = hash(key)%hashtable->num_buckets;
    bucketNode* bucketnode = hashtable->buckets[hashIndex];

    pthread_mutex_lock(&(bucketnode->bucketLock));

    entryNode* prev = NULL;
    entryNode* curr = hashtable->buckets[hashIndex]->firstNode;
        
    while(curr!=NULL){
        if(!strcmp(curr->key,key)){
            if(prev==NULL){
                hashtable->buckets[hashIndex]=NULL;
            }
            else{
                prev->next = curr->next;
            }
            hashtable->num_entries--;
            free(curr);
            pthread_mutex_unlock(&(bucketnode->bucketLock));        
            return true;
        }
        prev=curr;
        curr=curr->next;
    }
    pthread_mutex_unlock(&(bucketnode->bucketLock));        
    return false;
}

char* get(char* key){
    int hashIndex = hash(key)%hashtable->num_buckets;
    bucketNode* bucketnode = hashtable->buckets[hashIndex];
    
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

void put(char* key, char* value){
    int hashIndex = hash(key)%hashtable->num_buckets;
    bucketNode* bucketnode = hashtable->buckets[hashIndex];

    pthread_mutex_lock(&(bucketnode->bucketLock));

    entryNode* prev = NULL;
    entryNode* curr = bucketNode->firstNode;
   
    while(curr!=NULL){
        if(!strcmp(curr->key,key)){
            strcpy(curr->value,value);
            pthread_mutex_unlock(&(bucketnode->bucketLock));        
            return;
        }
        prev=curr;
        curr=curr->next;
    }

    hashtable->num_entries++;

    entryNode* temp = createEntryNode(key,value);
    if(prev==NULL){
        hashtable->buckets[hashIndex]=temp;
    }
    else{
        prev->next=temp;
    }
    
    pthread_mutex_unlock(&(bucketnode->bucketLock));        

    if (getLoadFactor() > hashtable->max_load_factor){
        resize();
    }
}
