#include <stdio.h>      /* defines printf for tests */
#include <time.h>       /* defines time_t for timings in the test */
#include <stdint.h>     /* defines uint32_t etc */
#include <sys/param.h>  /* attempt to define endianness */
#ifdef linux
# include <endian.h>    /* attempt to define endianness */
#endif

extern uint32_t hashlittle( const void *key, size_t length, uint32_t initval);
extern void hashlittle2(const void *key, size_t length, uint32_t *pc, uint32_t *pb);
extern uint32_t hashword(const uint32_t *k, size_t length, uint32_t initval);
extern void hashword2(const uint32_t *k, size_t length, uint32_t *pc, uint32_t *pb);

