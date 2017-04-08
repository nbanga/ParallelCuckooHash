#include <stdint.h>
#include <stdio.h>

uint32_t hashlittle( const void *key, size_t length, uint32_t initval);
void hashlittle2(const void *key, size_t length, uint32_t *pc, uint32_t *pb);
void hashword2(const uint32_t *k, size_t length, uint32_t *pc, uint32_t *pb);
