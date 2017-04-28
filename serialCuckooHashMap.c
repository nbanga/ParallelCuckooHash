#include "serialCuckooHashMap.h"

cuckooHashTable* cuckoohashtable;
int num_entries_per_iteration;

int compare(void *a, void *b){
    return *(int *) a - *(int *)b;
}

cuckooHashTable* createHashTable(int num_buckets){

    cuckooHashTable* hashtable = (cuckooHashTable *) malloc(sizeof(cuckooHashTable));
    hashtable->buckets = (entryNode **) malloc(num_buckets * sizeof(entryNode *));

    int i = 0, j = 0;
    for(; i<num_buckets; i++){
        hashtable->buckets[i] = (entryNode *) malloc (NUM_SLOTS * sizeof(entryNode));
        for(j = 0; j<NUM_SLOTS; j++){
            hashtable->buckets[i][j].key = NULL;
            hashtable->buckets[i][j].value = NULL;
        }
    }

    hashtable->num_buckets = num_buckets;
    hashtable->num_entries = 0;
    return hashtable;
}

void computehash(char* key, uint32_t *h1, uint32_t *h2){
    hashlittle2((void *)key, strlen(key), h1, h2);
}

void printHashTable(void){
    //printf("Print Hash Table\n");
    int buckets = cuckoohashtable->num_buckets;
    int i,j;
    entryNode *newNode;

    for(i=0; i<buckets; i++){
         newNode = cuckoohashtable->buckets[i];
         for(j=0; j<NUM_SLOTS;j++){
             if(newNode[j].key != NULL){
                 //printf("%s ",newNode[j].key);
             }
         }
    }
}

char* get(char* key){
    uint32_t h1 = 0, h2 = 0;
    entryNode* temp;
    int i;    

    computehash(key, &h1, &h2);
    //printf("GET H1: %d, H2: %d, key: %s\n", h1, h2, key);
    
    h1 = h1 % cuckoohashtable->num_buckets;
    temp = cuckoohashtable->buckets[h1];    
    for(i = 0; i< NUM_SLOTS; i++){
       if(temp[i].key != NULL && !strcmp(temp[i].key, key)){
           //printf("Get : Key : %s at [%d][%d]\n", temp[i].key, h1, i);
           return temp[i].value;
       }
    } 

    h2 = h2 % cuckoohashtable->num_buckets;
    temp = cuckoohashtable->buckets[h2];    
    for(i = 0; i< NUM_SLOTS; i++){
       if(temp[i].key != NULL && !strcmp(temp[i].key, key)){
          //printf("Get : Key : %s at [%d][%d]\n", temp[i].key, h2, i);
          return temp[i].value;
       }
    }
    
    //printf("Get : NOT Found key : %s\n", key);
    return NULL;
}

bool removeKey(char* key){
    uint32_t h1 = 0, h2 = 0;
    entryNode* temp;
    int i;    

    computehash(key, &h1, &h2);
    
    h1 = h1 % cuckoohashtable->num_buckets;
    temp = cuckoohashtable->buckets[h1];    
    for(i = 0; i< NUM_SLOTS; i++){
       if(temp[i].key != NULL && !strcmp(temp[i].key, key)){
           free(temp[i].key);
           free(temp[i].value);
           temp[i].key = NULL;
           temp[i].value = NULL;
           return true;
       }
    } 

    h2 = h2 % cuckoohashtable->num_buckets;
    temp = cuckoohashtable->buckets[h2];    
    for(i = 0; i< NUM_SLOTS; i++){
       if(temp[i].key != NULL && !strcmp(temp[i].key, key)){
           free(temp[i].key);
           free(temp[i].value);
           temp[i].key = NULL;
           temp[i].value = NULL;
           return true;
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
    entryNode *first, *second;
    entryNode evictentry;
    char *curr_key, *curr_value;
    int i;

    curr_key = key;
    curr_value = value;
    char* temp_key = (char *) malloc(sizeof(char) * MAX_SIZE);
    char* temp_value = (char *) malloc(sizeof(char) * MAX_SIZE);
    unsigned int t = 1;
    srand48((unsigned)t);  

    while(num_iterations < MAX_ITERATIONS){
        //printf("Num Iterations %d for key %s\n", num_iterations, key);
        num_iterations++;
        h1 = 0;
	h2 = 0;
	computehash(curr_key, &h1, &h2);
        //printf("PUT H1: %d, H2: %d, key: %s\n", h1, h2, curr_key);

        h1 = h1 % htptr->num_buckets;
        first = htptr->buckets[h1];    
        h2 = h2 % htptr->num_buckets;
        second = htptr->buckets[h2];
    
        for(i = 0; i< NUM_SLOTS; i++){
            if(first[i].key == NULL){
                first[i].key = (char *) malloc(sizeof(char) * MAX_SIZE);
                first[i].value = (char *) malloc(sizeof(char) * MAX_SIZE);
                strcpy(first[i].key, curr_key);
                strcpy(first[i].value, curr_value);
                //printf("First: Inserted Key %s in bucket %d, %d\n", key, h1, i); 
                return NULL;
            }
        }

        for(i = 0; i< NUM_SLOTS; i++){
            if(second[i].key == NULL){
                second[i].key = (char *) malloc(sizeof(char) * MAX_SIZE);
                second[i].value = (char *) malloc(sizeof(char) * MAX_SIZE);
                strcpy(second[i].key, curr_key);
                strcpy(second[i].value, curr_value);
                //printf("Second: Inserted Key %s in bucket %d, %d\n", key, h2, i);
                return NULL;
            }
        }

        int index = rand_r(&t) % (2*NUM_SLOTS);

        if(index >= 0 && index <NUM_SLOTS){
            evictentry = first[index];
        }
        else{
            evictentry = second[index - NUM_SLOTS];
        }

        //printf("Evicted Key %s with index %d\n", evictentry.key, index);
        strcpy(temp_key, evictentry.key);
        strcpy(temp_value, evictentry.value);

        strcpy(evictentry.key, curr_key);
        strcpy(evictentry.value, curr_value);
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
    //printf("In Resize\n");
    cuckooHashTable *newHashTable = createHashTable(num_buckets);
    int i, j;
    entryNode* res;

    for(i = 0; i < cuckoohashtable->num_buckets; i++){
        entryNode *bucketRow = cuckoohashtable->buckets[i]; 
        for(j=0; j < NUM_SLOTS; j++){
            if(bucketRow[j].key != NULL){
                res = _put(newHashTable, bucketRow[j].key, bucketRow[j].value);
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
    cuckoohashtable = newHashTable;
}

void put(char* key, char* value){
    char* valaddress = get(key);
    if(valaddress != NULL){
        //printf("Old : %s, New : %s\n", valaddress, value);
        strcpy(valaddress, value);
        return;
    }    
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
    printf("\nSerial CuckooHashMap\n");

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
