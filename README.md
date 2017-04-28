Parallelize hashmap implementation for the generic hashmap and cuckoo hashing scheme.
Introduces a global lock on resize
Implemented tags for better cache line utilization
Implemented lock free read with atomic operations, key version counters to ensure correct reads and a bitmap to track the evictions without restructuring

More details and results of this implementation can be found at:
https://docs.google.com/presentation/d/1KQKh1nN9dUL4PKJ5ey27O52leUnpGkWy-lQu30KkrMo/edit?ts=59027433#slide=id.g216a87d310_0_65
