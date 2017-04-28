#!/bin/bash

#chmod +777 serialHashTable

for i in 2 4 6 8 16 32
do
	./serialHashTable $i
done

for i in 2 4 6 8 16 32
do
	./parallelHashTable $i
done

for i in 2 4 6 8 16 32
do
	./serialCuckooHashMap $i
done

for i in 2 4 6 8 16 32
do
	./parallelCuckooHashMap $i
done

for i in 2 4 6 8 16 32
do
	./serialTagCuckooHashMap $i
done

for i in 2 4 6 8 16 32
do
	./parallelTagCuckooHashMap $i
done

for i in 2 4 6 8 16 32
do
	./lockFreeReadTaggedCuckooHashMap $i
done
