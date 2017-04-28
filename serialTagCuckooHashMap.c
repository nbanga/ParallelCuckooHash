#include "serialTagCuckooHashMap.h"

cuckooHashTable* cuckoohashtable;
int num_entries_per_iteration;

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
    int buckets = cuckoohashtable->num_buckets;
    int i,j;
    tagNode *newNode;

    for(i=0; i<buckets; i++){
        
         newNode = cuckoohashtable->buckets[i];
         for(j=0; j<NUM_SLOTS;j++){
             if(newNode[j].entryNodeptr != NULL){
                 //printf("%d %s; ",newNode[j].tag, newNode[j].entryNodeptr->key);
             }
         }
         //printf("\n");
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
    
    //printf("GET H1: %d, H2: %d, key: %s\n", h1, h2, key);
    
    for(i = 0; i< NUM_SLOTS; i++){
        
        //printf("Temp1 Tag: %d, My Tag : %d\n", first[i].tag, tag);
        if(first[i].tag == tag && first[i].entryNodeptr!=NULL){
           //printf("Temp Val: %s\n", first[i].entryNodeptr->key);
           entryNode* entrynode = first[i].entryNodeptr;
           if(!strcmp(key,entrynode->key)){
               //printf("Get : Key : %s at [%d][%d]\n", first[i].entryNodeptr->key, h1, i);
               return entrynode->value;
           }
       }
    } 

   h2 = h2 % cuckoohashtable->num_buckets;
   second = cuckoohashtable->buckets[h2];    
   
   for(i = 0; i< NUM_SLOTS; i++){

       //printf("Temp2 Tag: %d, My Tag : %d\n", second[i].tag, tag);
       if(second[i].tag == tag && second[i].entryNodeptr!=NULL){
           //printf("Temp Val: %s\n", second[i].entryNodeptr->key);
           entryNode* entrynode = second[i].entryNodeptr;
           if(!strcmp(key,entrynode->key)){
               //printf("Get : Key : %s at [%d][%d]\n", second[i].entryNodeptr->key, h1, i);
               return entrynode->value;
           }
       }
    }
    
    //printf("Get : NOT Found key : %s\n", key);
    return NULL;
}

bool removeKey(char* key){
    uint32_t h1 = 0, h2 = 0;
    int i;    
    unsigned char tag;
    tagNode *first, *second;

    computehash(key, &h1, &h2);
    
    tag = hashTag(h1);    
    h1 = h1 % cuckoohashtable->num_buckets;
    first = cuckoohashtable->buckets[h1];    
    
    //printf("GET H1: %d, H2: %d, key: %s\n", h1, h2, key);
   
    
    for(i = 0; i< NUM_SLOTS; i++){
       if(first[i].tag == tag && first[i].entryNodeptr!=NULL){
           entryNode* entrynode = first[i].entryNodeptr;
           if(!strcmp(entrynode->key, key)){
               free(entrynode->key);
               free(entrynode->value);
               free(entrynode);
               first[i].entryNodeptr=NULL;
               return true;
           }
       }
    } 

    h2 = h2 % cuckoohashtable->num_buckets;
    second = cuckoohashtable->buckets[h2];    
    
    for(i = 0; i< NUM_SLOTS; i++){
        if(second[i].tag == tag && second[i].entryNodeptr!=NULL){
           entryNode* entrynode = second[i].entryNodeptr;
           if(!strcmp(entrynode->key, key)){
               free(entrynode->key);
               free(entrynode->value);
               free(entrynode);
               second[i].entryNodeptr=NULL;
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
        //printf("Num Iterations %d for key %s\n", num_iterations, key);
        num_iterations++;
        h1 = 0; h2 = 0;
        computehash(curr_key, &h1, &h2);

        tag = hashTag(h1);    
        h1 = h1 % htptr->num_buckets;
        
        first = htptr->buckets[h1];    
        
        //printf("PUT H1: %d, H2: %d, key: %s\n", h1, h2, curr_key);

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
                return NULL;
            }
            else if(!strcmp(first[i].entryNodeptr->key,curr_key)){
                strcpy(first[i].entryNodeptr->value,curr_value);
                return NULL;
            }
        }
        
        h2 = h2 % htptr->num_buckets;
        second = htptr->buckets[h2];

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

        //printf("Evicted Key %s with index %d\n", evictentry.entryNodeptr->key, index);
        strcpy(temp_key, evictentry.entryNodeptr->key);
        strcpy(temp_value, evictentry.entryNodeptr->value);

        evictentry.tag = tag;
        strcpy(evictentry.entryNodeptr->key, curr_key);
        strcpy(evictentry.entryNodeptr->value, curr_value);
        //printHashTable();
        
        strcpy(curr_key, temp_key);
        strcpy(curr_value, temp_value);
    }
    
    entryNode* finalevictedNode = (entryNode *) malloc(sizeof(entryNode));
    finalevictedNode->key = curr_key;
    finalevictedNode->value = curr_value;
    return finalevictedNode;
}

void resize(int num_buckets){
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
        //printf("Inside Resize: Free and Resize\n");
        //printHashTable();
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
        //printHashTable();
        resize((cuckoohashtable->num_buckets) * 2);
        _put(cuckoohashtable, res->key, res->value);
        //printHashTable();
    }
    
    //printf("Put a New value : %s\n", value);
    return;
}

int main(int argv, char** argc){

    int iterations = atoi(argc[1]);
    int num_buckets = TOTAL_BUCKETS;
    num_entries_per_iteration = TOTAL_ENTRIES/iterations;
    struct timeval init, end;
    struct timezone tz;
    double start_time, end_time;
    
    cuckoohashtable = createHashTable(num_buckets);

    char* keys = (char*) malloc(1000*sizeof(char)); 
    int i, j, count=0;
    printf("\nSerial Tagged CuckooHashMap\n");

    printf("Preloading data\n");

    for(j = 0; j < iterations; j++){ 
    	for(i=0;i<num_entries_per_iteration;i++){
        	snprintf(keys,10,"%d",i + (10000 * j));
        	put(keys, keys);
    	}
    }

    gettimeofday(&init, &tz);
    start_time = (double)init.tv_sec + (double) init.tv_usec / 1000000.0;

    for(j = 0; j < iterations; j++){ 
    	for(i=0;i<num_entries_per_iteration;i++){
        	snprintf(keys,10,"%d",i + (100 * j));
        	get(keys);
    	}
    }

    gettimeofday(&end, &tz);
    end_time = (double) end.tv_sec + (double) end.tv_usec / 1000000.0;
    printf("GET Time Taken:   %.9lf\n", end_time - start_time);

    gettimeofday(&init, &tz);
    start_time = (double)init.tv_sec + (double) init.tv_usec / 1000000.0;

    for(j = 0; j < iterations; j++){ 
    	for(i=0;i<num_entries_per_iteration;i++){
        	snprintf(keys,10,"%d",i + (100 * j));
        	get(keys);
		count++;
		if(count == 25){
			count = 0;
        		snprintf(keys,10,"%d",i + (10000 * j));
			put(keys, keys);
		}
    	}
    }
 
    gettimeofday(&end, &tz);
    end_time = (double) end.tv_sec + (double) end.tv_usec / 1000000.0;
    printf("GETPUT Time Taken:   %.9lf\n", end_time - start_time);
   
    return 0;
}
