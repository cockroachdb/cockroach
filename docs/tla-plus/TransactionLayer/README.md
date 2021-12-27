# Motivation
Under serializable isolation level, when transactions conflict, CRDB adopts some conflict handling strategies, It makes sense to verify the correctness of these conflict handling strategies using tla-plus.
# Method
This method is mainly divided into three parts:  
1.Init three transactions.  
2.Use CRDB's strategy to handle these three transactions.  
3.Check invariants and properties during and at the end of the run.  
## Init three transactions

1.Each ***Transaction*** contain 3~5 commands.Each command is a ***operation*** on a ***key***(e.g., **Read A** , **Write B 3**).  
2.***Operation*** in Operatins(e.g., {Write,Read}).  
3.***Key*** in KEYS(e.g., {"A","B"}).  
4.The ***Value*** of each command is unique and can only be used when the operation is write.    
5.For convenience, ***Transactions*** was defined at the begining, and use "exe_id" to indicate which command should be executed next.

- A defined set of three transactions is a initial state, we can change an operation in a transaction to get a new initial state.  
- E.g., if transaction-2 and transaction-3 is definite, and transaction-1 in specification is:  
InitT1 ==/\ op1 \in {Write} /\ op2 \in {Read} /\ op3 \in {Write, Read}  
　　　　/\ key1 \in {"A"}   /\ key2 \in {"A"} /\ key3 \in {"B"}  
　　　　/\ T1 = << [op    |-> op1, key   |-> key1, value |-> 1],  
　　　　　　　　　[op    |-> op2, key   |-> key2, value |-> 2],  
　　　　　　　　　[op    |-> op3, key   |-> key3, value |-> 3]>>  
Tansaction1 has 2 case :  
　　Write A 1, Read A, Write B 3.  
　　Write A 1, Read A, Read B.  
Thus we have 2 initial state because of 2 cases of transaction-1.  

## Use CRDB's strategy to handle these three transactions. 
### 5 state transitions are defined in specification to handle transactions. It mainly includes:  
- Begin a transaction and record information to transaction record(**BeginTransaction**).  
- Using write pipeline to execute commands in every transations(**PipeLineWrite**).  
- When all commands in a transaction is completed, its status should be changed to "staging"(**StartParallelCommit**), then check whether all inflight write have successfully persisted, if it is true, its status should be changed to "completed"(**CheckInflightAndCommit**).  
- The write intent of the committed transaction can be persisted to MVCC data at any time(**CleanIntentWrite**).  

### In the pipelinewrite section, transaction conflicts may occur, The processing logic is as follows:  
#### Before read, you should know: for convenience, I made the following changes:  
- When timestamp of a transaction is pushed, directly retry it, without refresh read or something else.
- No crash scenarios were designed.
- You can find more in :  https://www.cockroachlabs.com/docs/v21.2/architecture/transaction-layer#concurrency-control and https://github.com/cockroachdb/cockroach/blob/master/docs/design.md
![framework](https://user-images.githubusercontent.com/62065552/146865735-ec42d028-d61f-45c0-acb9-86a24472180d.png)
  
- Handle conflict 1 and handle conflict 2 will be described in detail below.

#### Handle conflict 1
![readconflict](https://user-images.githubusercontent.com/62065552/146737545-206f312f-47c9-4fc8-b17c-0562c1d83cd6.png)

#### Handle conflict 2
![writeconflict](https://user-images.githubusercontent.com/62065552/146737690-ca539b29-876c-4500-854c-6c608c5c0656.png)

## Check invariants and properties during and at the end of the run.  
There are many invariants and properties, and the most important one is:  
***Strategy of handling conflict is correct.***
  
Because of serializable isolation level, the transaction execution result shall be consistent with the result executed in ascending timestamp order.
Thus, transactions are sorted in ascending timestamp order.

E.g.,three transactions and timestamp when them end(if all "committed") are as follows:  
![ts](https://user-images.githubusercontent.com/62065552/147448238-70d43741-f324-4e9e-9753-ff5f928bfc6c.png)

Then, we can get a sequence in ascending timestamp orde.  
![readresult](https://user-images.githubusercontent.com/62065552/146861113-cf968e5e-e2bb-49d1-89f4-febe3d08cdb4.png)


Then, this sequence is used to get oracle.   
Finally, oracle, read_result and END is used to verify the correctness:  
- Read_result[i] should be euqal to oracle.(e.g.,Read_result[2] should be equal to sequence-<<5,0>>).
- The last write determines the final value.(e.g., because the last value written to "A" is 1 and "B" is 3, so in the end "A" and "B" in MVCCData with highest timestamp should be (1,3)).
# Result
There are 16 initial states in this specification.  
Eventually found 50 million states and took five minutes without violation of invariants and properties.  

# Description of modeling
If you are going to read tla-plus specification, the following may help you:  
- The timestamps of any two transactions are not equal at any time.  
- System timestamp is increased only after use, which is used to set timestamp of transaction.  
- The Read_result of a "aborted" transaction is meaningless, so when retry a transaction I'll empty its Read_result.  
- You can change InitTransactions to verify more initial states(e.g.,change **op1 \in {Write}** to **op1 \in {Write,Read}**, change **key1 \in {"A"}** to **key1 \in {"A","B"}**).  
- This transaction will be "aborted" when the number of retries reaches MaxAttempt.  
- Comparing priority and checking write persistence is not modeled, but traverse each state, in other word, No matter which transaction has high priority and whether inflight is successful or not, the processing strategy needs to be correct.  

# Todo List
Reduce state space.  
Add scenarios such as refresh read before retry.  



