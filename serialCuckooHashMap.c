#include "serialCuckooHashMap.h"

cuckooHashTable* cuckoohashtable;

void createHashTable(int num_buckets, float load_factor){

    cuckoohashtable = (cuckooHashTable *) malloc(sizeof(cuckooHashTable));
    cuckoohashtable->buckets = (entryNode **) malloc(num_buckets * sizeof(entryNode *));

    int i = 0, j = 0;
    for(; i<num_buckets; i++){
        cuckoohashtable->buckets[i] = (entryNode *) malloc (NUM_SLOTS * sizeof(entryNode));
        for(j = 0; j<NUM_SLOTS; j++){
            cuckoohashtable->buckets[i][j].key = NULL;
            cuckoohashtable->buckets[i][j].value = NULL;
        }
    }

    cuckoohashtable->num_buckets = num_buckets;
    cuckoohashtable->num_entries = 0;
    return;
}

void computehash(char* key, uint32_t *h1, uint32_t *h2){
    if(cuckoohashtable->hashFunction == HASHLITTLE2){
        hashlittle2((void *)key, strlen(key), h1, h2);
    }
    else{
        //hashword2(key, strlen(key), h1, h2); 
    }
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
           return temp[i].value;
       }
    } 

    h2 = h2 % cuckoohashtable->num_buckets;
    temp = cuckoohashtable->buckets[h2];    
    for(i = 0; i< NUM_SLOTS; i++){
       if(temp[i].key != NULL && !strcmp(temp[i].key, key)){
          return temp[i].value;
       }
    }
    
    return NULL;
}

void put(char* key, char* value){
    int num_iterations = 0;
    uint32_t h1, h2;
    entryNode *first, *second;
    entryNode evictentry;
    char *curr_key, *curr_value;
    int i;
 
    char* valaddress = get(key);
    if(valaddress != NULL){
        strcpy(valaddress, value);
        return;
    }
    
    curr_key = key;
    curr_value = value;
    char* temp_key = (char *) malloc(sizeof(char) * MAX_SIZE);
    char* temp_value = (char *) malloc(sizeof(char) * MAX_SIZE);
    
    while(num_iterations < MAX_ITERATIONS){
        num_iterations++;
        computehash(curr_key, &h1, &h2);
        
        h1 = h1 % cuckoohashtable->num_buckets;
        first = cuckoohashtable->buckets[h1];    
        h2 = h2 % cuckoohashtable->num_buckets;
        second = cuckoohashtable->buckets[h2];
    
        for(i = 0; i< NUM_SLOTS; i++){
            if(first[i].key == NULL){
                first[i].key = (char *) malloc(sizeof(char) * MAX_SIZE);
                first[i].value = (char *) malloc(sizeof(char) * MAX_SIZE);
                strcpy(first[i].key, curr_key);
                strcpy(first[i].value, curr_value);
                return;
            }
        }

        for(i = 0; i< NUM_SLOTS; i++){
            if(second[i].key == NULL){
                second[i].key = (char *) malloc(sizeof(char) * MAX_SIZE);
                second[i].value = (char *) malloc(sizeof(char) * MAX_SIZE);
                strcpy(second[i].key, curr_key);
                strcpy(second[i].value, curr_value);
                return;
            }
        }

        int index = rand() % (2*NUM_SLOTS);

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
        
        curr_key = temp_key;
        curr_value = temp_value;
    }

    rehash();
    put(curr_key,curr_value);
}

int main(void)
{
        return 0;


}
