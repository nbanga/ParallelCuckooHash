1_1: 
	gcc serialHashTable.c -o serialHashTable
	gcc -pthread parallelHashTable.c lookup3.c -o parallelHashTable
	gcc serialCuckooHashMap.c lookup3.c -o serialCuckooHashMap
	gcc -pthread parallelCuckooHashMap.c lookup3.c -o parallelCuckooHashMap
	gcc serialTagCuckooHashMap.c lookup3.c -o serialTagCuckooHashMap
	gcc -pthread parallelTagCuckooHashMap.c lookup3.c -o parallelTagCuckooHashMap
	gcc -pthread lockFreeReadTaggedCuckooHashMap.c lookup3.c -o lockFreeReadTaggedCuckooHashMap
	sh project.bash

clean:
	-rm -rf *.o $(OBJ) *~ serialHashTable parallelHashTable serialCuckooHashMap parallelCuckooHashMap serialTagCuckooHashMap parallelTagCuckooHashMap lockFreeReadTaggedCuckooHashMap
