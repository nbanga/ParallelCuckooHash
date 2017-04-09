#include "serialCuckooHashMap.h"

cuckooHashTable* cuckoohashtable;

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

char* get(char* key){
    uint32_t h1 = 0, h2 = 0;
    entryNode* temp;
    int i;    

    computehash(key, &h1, &h2);
    
    h1 = h1 % cuckoohashtable->num_buckets;
    temp = cuckoohashtable->buckets[h1];    
    for(i = 0; i< NUM_SLOTS; i++){
       if(temp[i].key != NULL && !strcmp(temp[i].key, key)){
           printf("Get : Found value : %s\n", temp[i].value);
           return temp[i].value;
       }
    } 

    h2 = h2 % cuckoohashtable->num_buckets;
    temp = cuckoohashtable->buckets[h2];    
    for(i = 0; i< NUM_SLOTS; i++){
       if(temp[i].key != NULL && !strcmp(temp[i].key, key)){
          printf("Get : Found value : %s\n", temp[i].value);
          return temp[i].value;
       }
    }
    
    printf("Get : NOT Found key : %s\n", key);
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

int _put(cuckooHashTable* htptr, char *key, char *value){
    int num_iterations = 0;
    uint32_t h1, h2;
    entryNode *first, *second;
    entryNode evictentry;
    char *curr_key, *curr_value;
    int i;

    curr_key = key;
    curr_value = value;
    char* temp_key = (char *) malloc(sizeof(char) * MAX_SIZE);
    char* temp_value = (char *) malloc(sizeof(char) * MAX_SIZE);
    
    while(num_iterations < MAX_ITERATIONS){
        printf("Num Iterations %d for key %s\n", num_iterations, key);
        num_iterations++;
        computehash(curr_key, &h1, &h2);
        
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
                printf("Inserted Key %s in bucket %d\n", key, h1); 
                return 0;
            }
        }

        for(i = 0; i< NUM_SLOTS; i++){
            if(second[i].key == NULL){
                second[i].key = (char *) malloc(sizeof(char) * MAX_SIZE);
                second[i].value = (char *) malloc(sizeof(char) * MAX_SIZE);
                strcpy(second[i].key, curr_key);
                strcpy(second[i].value, curr_value);
                printf("Inserted Key %s in bucket %d\n", key, h1);
                return 0;
            }
        }

        int index = rand() % (2*NUM_SLOTS);

        if(index >= 0 && index <NUM_SLOTS){
            evictentry = first[index];
        }
        else{
            evictentry = second[index - NUM_SLOTS];
        }

        printf("Evicted Key %s with index %d\n", evictentry.key, index);
        strcpy(temp_key, evictentry.key);
        strcpy(temp_value, evictentry.value);

        strcpy(evictentry.key, curr_key);
        strcpy(evictentry.value, curr_value);
        
        curr_key = temp_key;
        curr_value = temp_value;
    }
    return 1;
}

void resize(int num_buckets){
    printf("In Resize\n");
    cuckooHashTable *newHashTable = createHashTable(num_buckets);
    int i, j, res = 0;
    for(i = 0; i < cuckoohashtable->num_buckets; i++){
        entryNode *bucketRow = cuckoohashtable->buckets[i]; 
        for(j=0; j < NUM_SLOTS; j++){
            res = _put(newHashTable, bucketRow[j].key, bucketRow[j].value);
            if(res == 1){
                break;
            }
        }
        if(res == 1)
            break;        
    }
    if(res == 1){
        printf("Inside Resize: Free and Resize\n");
        freeHashTable(newHashTable);
        resize(2 * num_buckets);  
    }
    cuckoohashtable = newHashTable;
}

void put(char* key, char* value){
    char* valaddress = get(key);
    if(valaddress != NULL){
        printf("Old : %s, New : %s\n", valaddress, value);
        strcpy(valaddress, value);
        return;
    }
    
    int result = _put(cuckoohashtable, key, value);
   
    if(result){
        resize((cuckoohashtable->num_buckets) * 2);
        _put(cuckoohashtable, key, value);
    }
    printf("Put a New value : %s\n", value);
    return;
}


int main(void){
    cuckoohashtable = createHashTable(10);
    printf("createHashTable: num_buckets %d\n", cuckoohashtable->num_buckets);

    char** keys = (char**)malloc(20*sizeof(char*)); 
    printf("allocated keys\n");
    char* value = (char*)malloc(20*sizeof(char)); 
    printf("allocated values\n");
    
    int i;
    for(i=0;i<10;i++){
        keys[i] = (char*)malloc(10*sizeof(char));
        //printf("allocated key %d\n",i);
        snprintf(keys[i],10,"%d",i);
        //printf("key%d: %s\n", i, keys[i]);
        snprintf(value,20,"%d%s",i,"values");
        //printf("value: %s\n",value);
        put(keys[i],value);
        //printf("put in entry %d\n", i);
    }

    printf("num_entries after put = %d\n", cuckoohashtable->num_entries);
    printf("Get\n");
    for(i=0;i<10;i++){
        printf("key: %s, value: %s\n", keys[i],get(keys[i]));
    }

    printf("testing resize\n");
    for(i=11;i<20;i++){
        keys[i] = (char*)malloc(10*sizeof(char));
        snprintf(keys[i],10,"%d",i);
        snprintf(value,20,"%d%s",i,"values");
        put(keys[i],value);
    }
    
    printf("createHashTable: num_buckets %d\n", cuckoohashtable->num_buckets);

    for(i=0;i<5;i++){
        printf("remove key %s : %d\n", keys[i], removeKey(keys[i]));
    }


    printf("remove key %s : %d\n", "21", removeKey("21"));
    return 0;
}
