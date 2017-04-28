#include "serialHashTable.h"

hashTable* hashtable;
int num_entries_per_iteration;

int hash(char* key){
    unsigned long int hashval=1;
    int i = 0;

    /* Convert string to an integer */
    while(hashval < INT_MAX && i < strlen(key)) {
        hashval = hashval << 8;
        hashval += key[i];
        i++;
    }
    return hashval;    
}

void createHashTable(int num_buckets, float max_load_factor){
    hashtable = (hashTable*)malloc(1*sizeof(hashTable));
    hashtable->buckets = (entryNode**)malloc(num_buckets*sizeof(entryNode*));
    int i;
    for(i=0;i<num_buckets;i++){
        hashtable->buckets[i] = NULL;
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
    newEntryNode->key = (char*)malloc(sizeof(char)*MAX_SIZE);
    newEntryNode->value = (char*)malloc(sizeof(char)*MAX_SIZE);
    strcpy(newEntryNode->key,key);
    strcpy(newEntryNode->value,value);
    newEntryNode->next = NULL;
    return newEntryNode;   
}

bool removeKey(char* key){
    int hashIndex = hash(key)%hashtable->num_buckets;
    entryNode* prev = NULL;
    entryNode* curr = hashtable->buckets[hashIndex];
        
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
            return true;
        }
        prev=curr;
        curr=curr->next;
    }
    return false;
}

char* get(char* key){
    int hashIndex = hash(key)%hashtable->num_buckets;
    entryNode* temp = hashtable->buckets[hashIndex];
    while(temp!=NULL){
        if(!strcmp(temp->key,key)){
            return temp->value;
        }
        temp=temp->next;
    }
    return NULL;
}

void resize() {
    printf("Resize\n");
    entryNode** old_buckets = hashtable->buckets;
    int old_num_buckets = hashtable->num_buckets;
    
    hashtable->num_buckets *= 2;
    hashtable->num_entries = 0;
    hashtable->buckets = (entryNode**)malloc(hashtable->num_buckets*sizeof(entryNode*));
    
    int i;
    for(i=0; i<hashtable->num_buckets;i++){
        hashtable->buckets[i]=NULL;
    }
    
    for(i=0;i<old_num_buckets;i++){
        entryNode* curr = old_buckets[i];
        entryNode* temp;
        while(curr!=NULL){
            temp=curr;
            put(curr->key,curr->value);
            curr=curr->next;
            free(temp);
        }
    }
    
    free(old_buckets);
    return;
}

void put(char* key, char* value){
    int hashIndex = hash(key)%hashtable->num_buckets;
    entryNode* prev = NULL;
    entryNode* curr = hashtable->buckets[hashIndex];

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
        hashtable->buckets[hashIndex]=temp;
    }
    else{
        prev->next=temp;
    }

    if (getLoadFactor() > hashtable->max_load_factor){
        resize();
    }
}

int main(int argv, char** argc){

    int iterations = atoi(argc[1]);
    int num_buckets = TOTAL_BUCKETS;
    num_entries_per_iteration = (int) TOTAL_ENTRIES/iterations;
    struct timeval init, end;
    struct timezone tz;
    double start_time, end_time;
    
    createHashTable(num_buckets,1.5);
    printf("\nSerial HashMap\n");

    char* keys = (char*) malloc(1000*sizeof(char)); 
    int i, j, count=0;

    printf("Preloading data\n");
    
    for(j = 0; j < iterations; j++){ 
    	for(i=0;i<num_entries_per_iteration;i++){
        	snprintf(keys,10,"%d",i + (100000 * j));
        	put(keys, keys);
    	}
    }

    gettimeofday(&init, &tz);
    start_time = (double)init.tv_sec + (double) init.tv_usec / 1000000.0;

    for(j = 0; j < iterations; j++){ 
    	for(i=0;i<num_entries_per_iteration;i++){
        	snprintf(keys,10,"%d",i + (1000 * j));
        	get(keys);
    	}
    }

    gettimeofday(&end, &tz);
    end_time = (double) end.tv_sec + (double) end.tv_usec / 1000000.0;
    printf("Get Time Taken :   %.9lf\n", end_time - start_time);

    gettimeofday(&init, &tz);
    start_time = (double)init.tv_sec + (double) init.tv_usec / 1000000.0;

    for(j = 0; j < iterations; j++){ 
    	for(i=0;i<num_entries_per_iteration;i++){
        	snprintf(keys,10,"%d",i + (1000 * j));
        	get(keys);
		count++;
		if(count == 25){
			count = 0;
        		snprintf(keys,10,"%d",i + (100000 * j));
			put(keys, keys);
		}
    	}
    }
 
    gettimeofday(&end, &tz);
    end_time = (double) end.tv_sec + (double) end.tv_usec / 1000000.0;
    printf("GetPut Time Taken:   %.9lf\n", end_time - start_time);
   
    return 0;
}
