#include "serialHashTable.h"

hashTable* hashtable;

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
    //printf("createEntryNode: key %s\n", newEntryNode->key);
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
    //TODO: calculate time
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
    printf("%s %s\n", key, value);
    int hashIndex = hash(key)%hashtable->num_buckets;
    entryNode* prev = NULL;
    entryNode* curr = hashtable->buckets[hashIndex];

    printf("hashIndex: %d\n", hashIndex);

    while(curr!=NULL){
        if(!strcmp(curr->key,key)){
            strcpy(curr->value,value);
            return;
        }
        prev=curr;
        curr=curr->next;
    }

    hashtable->num_entries++;
    //printf("hashtable num_entries: %d\n", hashtable->num_entries);

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

int main(void){
    createHashTable(10,1.5);
    printf("createHashTable: num_buckets %d\n", hashtable->num_buckets);

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

    printf("num_entries after put = %d\n", hashtable->num_entries);
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
    
    printf("createHashTable: num_buckets %d\n", hashtable->num_buckets);

    for(i=0;i<5;i++){
        printf("remove key %s : %d\n", keys[i], removeKey(keys[i]));
    }

    printf("remove key %s : %d\n", "21", removeKey("21"));
    return 0;
}













