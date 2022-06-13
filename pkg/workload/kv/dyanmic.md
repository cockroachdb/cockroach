Dyanmic Access Distribution Workload

how does the workload look on the hotkey visualizer? *single node

workload: [hotkey, shuffle1, 1/10shuffle, 5period]
env:      single node local
splits:   [0, 256, 1048, 4096]

- how does crdb perform? 
    - allocation algorithm
        - are resources saturated
        - are resources balanced
        - what is the cost of balancing
    - workload
        - latency
        - throughput 
- environment
    - 9 node
    - n1-standard-8
- document findings
    - summary
    - introduction
      - design
      - questions
    - method
    - results
    - conclusion
    - repro 

Whenever

- move dynamic.go into a package, move the specific generators into a kv.go 
  - maybe separate out into files for function? probs not rly needed
    - triggers.go
    - slots.go
    - dynamic.go
    - distribution.go
  - update comments
  - move this into a doc.go in dynamic package
- pr

Introduction

The current testing workloads available in CRDB are static in terms of their
access distribution. They will access keys with identical probability during
their run.

In some customer workloads however, this is not the case. The probability
distribution of key accesses is not static, rather it dynamically changes as
time progresses. This presents a challenge in testing the database sufficiently
for scenarios that customers may encounter. The Dynamic Access Distribution
(DAD) is targeted at this issue.


Terminology

distribution: the probability that each key, within the keyspace is selected for an operation.
workload:     a synthetic client that issues operations against the database.
dynamic:      changing, specifically in this context it refers to something which may change.
static:       fixed, in this context something that cannot change.


Examples

1) Writes are unique and in lexiographically ordered w.r.t time. Reads occur
only on the most X recently written keys. This is coloqiually called a moving
hotkey.


```
     k
      
     |_ _ _ _ 5 3 1 
     |_ _ _ 5 3 1 _ 
keys |_ _ 5 3 1 _ _ 
     |_ 5 3 1 _ _ _
     |5 3 1 _ _ _ _ 
     +------------ t
           time
```

2) Key accesses are concerntrated in one location in the keyspace during
nightime, during day time they are located in a non-overlapping section. This
could be extended to many locations, where the location of access is some
function of time f(t) -> t > 12 : lower half, t < 12 upper half.

```
     k
      
     |_ _ _ _ 3 3 3
     |_ _ _ _ 3 3 3
keys |_ _ _ _ _ _ _ 
     |3 3 3 _ _ _ _
     |3 3 3 _ _ _ _ 
     +------------ t
           time
```


3) dynanmic key (sequenced). Keys move randomly in chunks, a whole chunk moves
together to a new position. The underlying distribution (zipf,seq,uniform)
affects the hotness of each key. This is similar to a moving hotkey, however
the mapping function is now more general.

```
     k
      
     |_ _ 1 1 3 3 _
     |_ _ 2 2 _ _ 1 
keys |1 1 3 3 _ _ 2 
     |2 2 _ _ 1 1 3
     |3 3 _ _ 2 2 _ 
     +-------------- t
           time
```

3) dynamic key (individual). Keys move randomly to new slots at each remapping.
The underlying distribution (zipf,seq,uniform) affects the hotness of each key.
This is expected to be the worst case workload for the distribution algorithm
to adapt to. This is the same as the case above, however with a chunk size = 1.

```
     k
      
     |1 3 1 _ 1 2 2 
     |2 1 3 2 _ _ 1 
keys |3 _ _ 1 2 1 _ 
     |_ 2 _ _ _ _ 3
     |_ _ 2 3 3 3 _ 
     +-------------- t
           time
```

Design

Parameters

A_r read access distribution   (zipf, hash, sequential, single, block)
W_r read distribution width    (int, slots, > 0)
N_r read num slots             (int, slots, > 0)
T_r read reconfigure interval  (int, seconds, > 1)
F_r read slot map function     (f(x)->x', 0 < x' < N_r)

A_w write access distribution  (zipf, hash, sequential, single, block)
W_w write distribution width   (int, slots, > 0)
N_w write num slots            (int, slots, > 0)
T_w write reconfigure interval (int, seconds, > 1)
F_w write slot map function    (f(x)->x', 0 < x' < N_w)

Examples

1) Moving Hotkey. 100 keys.

A_r: hash
W_r: 1
N_r: 100
T_r: 1s
F_r: f(x) -> x+1

A_w: hash
W_w: 10
N_w: 100
T_w: 1s
F_w: f(x) -> x+1

2) Seasonal

r=w

A: hash
W: 25
N: 100
F: f(x) -> (x + N\2) % N
T: 12hr

1) There is an issue of wrap around that keeps coming up.

does the slot range never change? .e.g. [1,2,3] could never become [2,3,4]?
only ever a permutation of 1,2,3. or are slots values allowed to change? e.g.
[1.2,3] could become [3,4,5].

In which case this somewhat defeats the purpose of having slots when you also
have sliding widths within the slots.

choice: 

a) slot ranges never change, distribution width acts as a bound for *active*
   queries [0, width], whilst the slots still can be rotated around [0, slots].
b) slot ranges change, distribution width is equal to the slot width for
   *active queries* [0, width], whilst the slot range is [x,x']. Where x'- x = slots.

examples:

moving hotkey - slots not needed
  a) large N_r,N_w, W_w=1, small W_r. f(x)->x+1
     t1 [0,1,2,3,4,5,6,7,8,9], width_w=[0] width_r=[0]
     ...
     t5 [5,6,7,8,9,0,1,2,3,4], width_w=[5],width_r=[5,4,3]

  b) N_w=1,small N_r, f(x)-> x+1
     t1 w=[0], r=[0,0,0]
     ...
     t5 w=[5], r=[5,4,3]

seasonal - slots not needed
  a) large N,small W, f(x)-> x + N/2 % N
  b) small N, f(x) -> x + ub/2 % ub?

chunked dyanmic - slots needed
  a) N=W, f(x) -> shuffle (x / chunk) 
  b) N, f(x) -> shuffe(x/chunk)

key dynamic - slots needed
  a) f(x) -> shuffle(x), x in [0,N]
  b) f(x) -> shuffle(x), x in [0,N]


2) Sequence and reading only keys that have already been written / underling
   access distribution generator


3) Another usecase is running multiple distributions at the same time, across
   different sections of the keyspace.

reads and writes are already separated by distrbution. However we could also
split sections of the slots or keyspace into different distributions?

