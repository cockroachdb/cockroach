---- MODULE TransactionLayer----

EXTENDS Naturals, FiniteSets, Sequences, TLC

\* In this Spec, I create three transactions(txn), each txn contain 3 ~ 5 commands.
\* Each command is Read or Write(operation) on a key.


CONSTANTS Read,Write
CONSTANTS Nil
\* Maximum number of attempt, beyond this, this txn will be aborted.
CONSTANTS MaxAttempt

\* Multi-version value of key.
VARIABLES MVCCData
\* Intent write of key.
VARIABLES Intent_write
\* Every txn's timestamp(ts), status, attempt count and command.
VARIABLES Record
VARIABLES System_ts
\* Every txn's result of Read.
VARIABLES Read_result
\* Index of command which should be executed next. 
VARIABLES Txn_exeid
\* Most recently used ts of each key.
VARIABLES Tscache
\* In the ParallelCommit, set that should be checked.
VARIABLES Tocheck


\* The following VARIABLES are used to create three txns, and will never be changed.
VARIABLES Transactions
VARIABLES T1,T2,T3
VARIABLES op1,op2,op3,op4,op5,op6,op7,op8,op9,opt,opj,opq
VARIABLES key1,key2,key3,key4,key5,key6,key7,key8,key9,keyt,keyj,keyq
ops  == <<op1,op2,op3,op4,op5,op6,op7,op8,op9,opt,opj,opq>>
keys == <<key1,key2,key3,key4,key5,key6,key7,key8,key9,keyt,keyj,keyq>>
Unchangedvars == <<Transactions,ops,keys,T1,T2,T3>>

\* All VARIABLES
vars == <<Unchangedvars,MVCCData,Record,System_ts,Read_result,Txn_exeid,Intent_write,Tscache,Tocheck>>

\* Keys in command.
KEYS == {"A","B"}

\* Initiate txn1.
\* in this case:
\* op              key        value(useless if Read)
\* Write           A          1
\* Read            A          2(useless)
\* Write or Read   B          3
\* Txn1 have 2 cases.
InitT1 == /\ op1 \in {Write} /\ op2 \in {Read} /\ op3 \in {Write,Read}
          /\ key1 \in {"A"}  /\ key2 \in {"A"} /\ key3 \in {"B"}
          /\ T1 = <<[op    |-> op1, key   |-> key1, value |-> 1],
                    [op    |-> op2, key   |-> key2, value |-> 2],
                    [op    |-> op3, key   |-> key3, value |-> 3]>>
                    
\* Initiate txn2.
\* in this case.
\* op              key        value(useless if Read)
\* Write or Read   A or B     4(useless if Read)
\* Write           A or B     5
\* Write           A          6
\* Read            A          7(useless)
\* Txn2 have 8 cases.                    
InitT2 == /\ op4 \in {Write,Read} /\ op5 \in {Write}   /\ op6 \in {Write}/\ op7 \in {Read}
          /\ key4 \in {"A","B"}  /\ key5 \in {"A","B"} /\ key6 \in {"A"} /\ key7 \in {"A"}
          /\ T2 = <<[op    |-> op4, key   |-> key4, value |-> 4],
                    [op    |-> op5, key   |-> key5, value |-> 5],
                    [op    |-> op6, key   |-> key6, value |-> 6],
                    [op    |-> op7, key   |-> key7, value |-> 7]>>
\* Initiate txn3.
\* in this case:
\* txn3 have 1 case.  
InitT3 == /\ op8 \in {Read} /\ op9 \in {Write} /\ opt \in {Write} /\ opj \in {Read} /\ opq \in {Read}
          /\ key8 \in {"A"} /\ key9 \in {"A"}  /\ keyt \in {"B"}  /\ keyj \in {"A"}  /\ keyq \in {"B"}
          /\ T3 = <<[op    |-> op8, key   |-> key8, value |-> 8],
                    [op    |-> op9, key   |-> key9, value |-> 9],
                    [op    |-> opt, key   |-> keyt, value |-> 10],
                    [op    |-> opj, key   |-> keyj, value |-> 11],
                    [op    |-> opq, key   |-> keyq, value |-> 12]>>
                    
\* There are 2*8*1 = 16 initial txns states.
InitTransactions == 
        /\ InitT1
        /\ InitT2
        /\ InitT3
        /\ Transactions = <<T1,T2,T3>>
                  
InitData == /\ MVCCData = [c \in KEYS |-> <<[TS  |-> 0, Value  |-> 0]>>]  
            /\ Intent_write = [i \in KEYS |-> Nil] 
    
InitRecoed == Record = <<>>

InitSign == /\ System_ts    = 1
            /\ Txn_exeid    = [i \in 1..3 |-> 1]
            /\ Tscache      = [i \in KEYS |-> 0] 
            /\ Tocheck      = <<>>

InitResult == Read_result = [i \in 1..3 |-> <<>>]

Init == /\ InitTransactions
        /\ InitData
        /\ InitRecoed
        /\ InitSign
        /\ InitResult

\* Get the value of key in MVCCData according to ts. 
GetLastNum(k,ts) == LET Indexs == {i \in 1..Len(MVCCData[k]):  MVCCData[k][i].TS <= ts}
                        maxIndex    == CHOOSE x \in Indexs : \A y \in Indexs : x >= y
                    IN MVCCData[k][maxIndex].Value
                    
\* Add something(a) to a tuple(s) on index(i).
SthIndexAdd(s,i,a) == [t \in DOMAIN s \union {i} |-> IF t \in DOMAIN s THEN s[t] ELSE a]


\* Beagin a txn.
BeginTransaction(tid) == 
               LET newT == [status |-> "pending", attempt |-> 0, ts |-> System_ts ,command |-> Transactions[tid]]
               IN 
                  /\ tid \notin DOMAIN Record
                  /\ Record' = SthIndexAdd(Record,tid,newT)
                  /\ System_ts' = System_ts + 1
                  /\ UNCHANGED <<Unchangedvars,MVCCData,Read_result,Txn_exeid,Intent_write,Tscache,Tocheck>>

\* Execute next command of a txn.
PipeLineWrite(tid) == 
            /\ Record[tid].status = "pending"
            /\ Txn_exeid[tid] <= Cardinality(DOMAIN Record[tid].command) 
            /\ LET cmd == Record[tid].command[Txn_exeid[tid]]
                  \* Judge whether retry or not accroding to ts cache.
               IN /\ \/ /\ Tscache[cmd.key] > Record[tid].ts
                        \* Failed ts cache check.
                        \* Retry this txn.
                        /\ Record' = [Record EXCEPT ![tid] = [ts      |-> System_ts,
                                                              \* Set status according to attempt.
                                                              status  |-> IF @.attempt < MaxAttempt THEN "pending" ELSE "aborted",
                                                              attempt |-> @.attempt + 1,
                                                              command |-> @.command]]
                        /\ System_ts' = System_ts + 1
                        /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = 1]
                        /\ Read_result' = [Read_result EXCEPT ![tid] = <<>>]
                        /\ UNCHANGED <<Tscache,MVCCData,Intent_write>>
                        
                     \/ /\ Tscache[cmd.key] <= Record[tid].ts
                        \* Pass ts cache check.
                        /\ Tscache' = [Tscache EXCEPT ![cmd.key] = Record[tid].ts]
                        
                        \* Judge whether exists intent write in key. 
                        /\ \/ /\ Intent_write[cmd.key] = Nil
                              \* Don't exist intent write.
                              \* Judge operation of this command.
                              /\ \/ /\ cmd.op = Read
                                    \* Operation is Read.
                                    \* Read the value of key in MVCCData according to ts.
                                    /\ Read_result' = [Read_result EXCEPT ![tid] = SthIndexAdd
                                                   (@,Len(@) + 1,GetLastNum(cmd.key,Record[tid].ts))]
                                    /\ UNCHANGED <<Intent_write>>
                                    
                                 \/ /\ cmd.op = Write
                                    \* Operation is Write.
                                    \* Write the key-value to intent write.
                                    /\ Intent_write' = [Intent_write EXCEPT ![cmd.key] = [txn_id |-> tid,
                                                                                          value  |-> cmd.value,
                                                                                          ts     |-> Record[tid].ts]]
                                    /\ UNCHANGED <<Read_result>>
                              /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = @ + 1]
                              /\ UNCHANGED <<Record,MVCCData,System_ts>>
                           \/ /\ Intent_write[cmd.key] /= Nil
                              \* Exist intent write.
                              \* Conflict handling.
                              /\ LET \* Content of intent write.
                                     ciw  == Intent_write[cmd.key]
                                     \* Conflicted txn's id.
                                     ctid == Intent_write[cmd.key].txn_id
                                     \* Conflicted txn's status.
                                     stat == Record[ctid].status
                                     \* Conflicted txn's ts.
                                     cts  == Record[ctid].ts
                                     
                                    \* Judge conflicted txn's status.
                                 IN /\ \/ /\ stat = "committed"
                                          \* Move the value from intent write to MVCCData.
                                          /\ MVCCData' = [MVCCData EXCEPT ![cmd.key] = 
                                                          SthIndexAdd(@,Len(@) + 1,[TS    |-> ciw.ts,
                                                                                    Value |-> Intent_write[cmd.key].value])]
                                          /\ Intent_write' = [Intent_write EXCEPT ![cmd.key] = Nil] 
                                          /\ UNCHANGED <<Read_result,Record,Txn_exeid,System_ts>>                                                  
                                       \/ /\ stat = "aborted"
                                          \* Delete this intent write.
                                          /\ Intent_write' = [Intent_write EXCEPT ![cmd.key] = Nil]
                                          /\ UNCHANGED <<MVCCData,Read_result,Record,Txn_exeid,System_ts>>
                                       \/ /\ stat = "staging"
                                          \* Wait for Conflicted txn.
                                          /\ FALSE
                                       \/ /\ stat = "pending"
                                          \* Conflict handling.
                                          \* Judge whether the intent write is written by this txn.
                                          /\ \/ /\ tid = ctid
                                                \* This intent write is written by this txn.
                                                /\ \/ /\ cmd.op = Write
                                                      \* Overwrite intent write.
                                                      /\ Intent_write' = [Intent_write EXCEPT ![cmd.key] = [txn_id |-> tid,
                                                                                                            value  |-> cmd.value,
                                                                                                            ts     |-> Record[tid].ts]]
                                                      
                                                      /\ UNCHANGED <<Read_result>>
                                                   \/ /\ cmd.op = Read
                                                      \* Judge whether the intent write conflict is caused by txn retry.
                                                      /\ \/ /\ ciw.ts = Record[tid].ts
                                                            \* Intent write conflict isn't caused by txn retry.
                                                            \* Read the value of key in MVCCData according to ts.
                                                            /\ Read_result' = [Read_result EXCEPT ![tid] = SthIndexAdd(@,Len(@) + 1,Intent_write[cmd.key].value)]
                                                            /\ UNCHANGED <<Intent_write>> 
                                                         \/ /\ ciw.ts < Record[tid].ts
                                                            \* Intent write conflict is caused by txn retry.
                                                            \* Clean intent write and read the value of key in MVCCData according to ts.
                                                            /\ Intent_write' = [Intent_write EXCEPT ![cmd.key] = Nil]
                                                            /\ Read_result' = [Read_result EXCEPT ![tid] = SthIndexAdd(@,Len(@) + 1,GetLastNum(cmd.key,Record[tid].ts))]
                                                            
                                                /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = @ + 1]
                                                /\ UNCHANGED <<Record,System_ts>>
                                                
                                             \/ /\ tid /= ctid 
                                                \* This intent write isn't written by this txn.
                                                /\ \/ /\ ciw.ts /= cts
                                                      \* Conflicted txn's ts is not equal to intent write's ts
                                                      \* This intent write is expired, clean it.
                                                      /\ Intent_write' = [Intent_write EXCEPT ![cmd.key] = Nil]
                                                      /\ UNCHANGED <<System_ts,Txn_exeid,Record,Read_result>>
                                                      
                                                   \/ /\ ciw.ts = cts
                                                      /\ \/ /\ cmd.op = Write 
                                                            /\ Record[tid].ts < cts
                                                            \* This is w-w conflict, abort this txn. 
                                                            /\ Record' = [Record EXCEPT ![tid].status = "aborted"]
                                                            /\ Read_result' = [Read_result EXCEPT ![tid] = <<>>]
                                                            /\ UNCHANGED <<System_ts,Txn_exeid>>
                                                         \/ /\ cmd.op = Read 
                                                            \* Compare txn's ts with conflict txn's ts.
                                                            /\ \/ /\ Record[tid].ts < cts
                                                                  \* Ignore this intent write, and read the value of key in MVCCData according to ts. 
                                                                  /\ Read_result' = [Read_result EXCEPT ![tid] = SthIndexAdd(@,Len(@) + 1,GetLastNum(cmd.key,Record[tid].ts))]
                                                                  /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = @ + 1]
                                                                  /\ UNCHANGED <<Record,System_ts>>
                                                            
                                                               \/ /\ Record[tid].ts > cts
                                                                  \* Push conflict txn's ts, and retry conflict txn.
                                                                  /\ Record' = [Record EXCEPT ![ctid] = [ts      |-> System_ts,
                                                                                                         status  |-> IF @.attempt < MaxAttempt THEN "pending" ELSE "aborted",
                                                                                                         attempt |-> @.attempt + 1,
                                                                                                         command |-> @.command]]
                                                                  /\ System_ts' = System_ts + 1
                                                                  /\ Read_result' = [Read_result EXCEPT ![ctid] = <<>>]
                                                                  /\ Txn_exeid' = [Txn_exeid EXCEPT ![ctid] = 1]
                                                      /\ UNCHANGED <<Intent_write>>                                           
                                                            
                                                
                                            /\ UNCHANGED <<MVCCData>>
                         
                  /\ UNCHANGED <<Unchangedvars,Tocheck>> 
                  
\* Commit a txn.               
StartParallelCommit(tid) == 
            \* All commands have been executed.
            /\ Txn_exeid[tid] = Cardinality(DOMAIN Record[tid].command) + 1
            \* Change txn's status from "pending" to "staging"
            /\ Record[tid].status = "pending" 
            /\ Record' = [Record EXCEPT ![tid].status = "staging"]
            /\ LET commands == Record[tid].command
                   len == Len(Record[tid].command) 
                   allwriteindex == {i \in 1..len: commands[i].op = Write}
                   allwrite == {commands[i] : i \in allwriteindex}
                   inflight == {[success |-> Nil,
                                 key     |-> i.key,
                                 value   |-> i.value]: i \in allwrite}
                  \* Use tocheck to store commands that need to be checked for successfully replicated.
               IN /\ Tocheck' = SthIndexAdd(Tocheck,tid,inflight)
                  /\ UNCHANGED <<Unchangedvars,MVCCData,System_ts,Read_result,Txn_exeid,Intent_write,Tscache>>

\* Check whether a command is successfully replicated.
CheckInflight(tid,command) == 
            /\ command.success = Nil
            /\ \/ \* Replication succeeded.
                  /\ Tocheck' = [Tocheck EXCEPT ![tid] = (Tocheck[tid] \ {command}) 
                                  \union {[key     |-> command.key,
                                           value   |-> command.value,
                                           success |-> TRUE]}]
                  /\ UNCHANGED <<Record,System_ts,Txn_exeid,Read_result>>
               \/ \* Replication failed.
                  \* There is no need to check other commands.
                  \* Retry this txn.
                  /\ Tocheck' = [i \in DOMAIN Tocheck \ {tid} |-> Tocheck[i]]
                  /\ Record' = [Record EXCEPT ![tid] = [ts      |-> System_ts,
                                                        status  |-> IF @.attempt < MaxAttempt THEN "pending" ELSE "aborted",
                                                        attempt |-> @.attempt + 1,
                                                        command |-> @.command]]
                  /\ System_ts' = System_ts + 1
                  /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = 1]
                  /\ Read_result' = [Read_result EXCEPT ![tid] = <<>>]
                  
            /\ UNCHANGED <<Unchangedvars,MVCCData,Intent_write,Tscache>>
             
\* Judge whether the txn meets the commit conditions.
JudgeCommit(tid) == 
            /\ Record[tid].status = "staging"
            \* All commands have been successfully replicated.
            /\ \A i \in Tocheck[tid] : i.success = TRUE
            /\ Record' = [Record EXCEPT ![tid].status = "committed"]
            /\ Tocheck' = [i \in (DOMAIN Tocheck) \ {tid} |-> Tocheck[i]] 
            /\ UNCHANGED <<Unchangedvars,MVCCData,Intent_write,Tscache,Read_result,Txn_exeid,System_ts>>


\* Move the value from intent write to MVCCData.
CleanIntentWrite(k) == 
         LET tid == Intent_write[k].txn_id
         IN /\ Intent_write[k] /= Nil
            /\ Record[tid].status = "committed"
            /\ Assert(Intent_write[k].ts = Record[tid].ts,"error in CleanIntentWrite")
            /\ Intent_write' = [Intent_write EXCEPT ![k] = Nil]
            /\ MVCCData' = [MVCCData EXCEPT ![k] = SthIndexAdd(@,Len(@) + 1,[TS    |-> Intent_write[k].ts,
                                                                             Value |-> Intent_write[k].value])]
            /\ UNCHANGED <<Unchangedvars,Record,System_ts,Read_result,Txn_exeid,Tscache,Tocheck>>                                                                 


Next == 
        \/ \E tid \in 1..3 : BeginTransaction(tid)
        \/ \E tid \in DOMAIN Record : PipeLineWrite(tid)
        \/ \E tid \in DOMAIN Record : StartParallelCommit(tid)
        \/ \E tid \in DOMAIN Tocheck : \E command \in Tocheck[tid] : CheckInflight(tid,command)
        \/ \E tid \in DOMAIN Tocheck : JudgeCommit(tid)
        \/ \E k \in KEYS : CleanIntentWrite(k)
        
       

                                        
\* Use SerializeedTxn, SerializedRead and lenth of SerializeedTxn to verify its correctness.
\* To verify its correctness, we verify:
\* 1.The read value is the most recently written value.
\* 2.The last written value determines the current value of MVCCData. 
CheckInv(SerializeedTxn,SerializedRead,num) == 
                 /\ \A index \in 1..num : LET Item == SerializeedTxn[index]
                                          IN \/ /\ Item.op = Write
                                                /\ (~ \E i \in (index+1)..num : /\ SerializeedTxn[i].op  = Write
                                                                                /\ SerializeedTxn[i].key = Item.key)
                                                   => GetLastNum(Item.key,999) = Item.value            
                                             \/ /\ Item.op = Read
                                                /\ LET samekeyW    == {i \in 1..(index-1) : /\ SerializeedTxn[i].op = Write
                                                                                            /\ SerializeedTxn[i].key = Item.key}
                                                       beforeRnum  == Cardinality({i \in 1..(index-1) : SerializeedTxn[i].op = Read})
                                                       thisR       == SerializedRead[beforeRnum + 1]
                                                             
                                                   IN \* Without written before, read 0.
                                                      \/ /\ samekeyW = {}
                                                         /\ thisR = 0
                                                      \* With written before, read recently written value.
                                                      \/ /\ samekeyW /= {}
                                                         /\ thisR = SerializeedTxn[CHOOSE x \in samekeyW : \A y \in samekeyW : x >= y].value

\* Return txn's command if committed.    
GetTxn(i) == IF Record[i].status = "committed" THEN Record[i].command 
             ELSE IF Record[i].status = "aborted" THEN <<>>
             ELSE Assert(FALSE,"error status")
\* Get lenth of txn's commands if committed.             
GetTxnLen(i,t) ==
             LET committedSetNum == Cardinality({m \in 1..3 : Record[m].status = "committed"})
             IN IF /\ Record[i].status = "committed" 
                   /\ t <= committedSetNum
                THEN Len(Record[i].command)
                ELSE 0

\* verify its correctness.      
invCorrect ==\* When all txns are terminated and all intent write is written to MVCCData, check its correctness.
             /\ Cardinality({id \in DOMAIN Record : Record[id].status = "committed"}) +
                Cardinality({id \in DOMAIN Record : Record[id].status = "aborted"}) = 3
             /\ \A k \in KEYS : Intent_write[k] = Nil 
           =>   LET  \* Sort 3 txns
                     \* 1.The committed txn precedes the aborted txn.
                     \* 2.If the two txn status are the same, the smaller ts is in front.
                     txn1 == CHOOSE x \in 1..3 : \A y \in 1..3 : IF Record[x].status /=  Record[y].status
                                                                 THEN Record[x].status = "committed"
                                                                 ELSE Record[x].ts <= Record[y].ts
                                                                 
                     txn3 == CHOOSE x \in 1..3 : \A y \in 1..3 : IF Record[x].status /=  Record[y].status
                                                                 THEN Record[x].status = "aborted"
                                                                 ELSE Record[x].ts >= Record[y].ts
                                                                 
                     txn2 == CHOOSE X \in 1..3 \ ({txn1} \union {txn3}) : TRUE
                     
                     
                     TL1 == GetTxnLen(txn1,1)
                     TL2 == GetTxnLen(txn2,2)
                     TL3 == GetTxnLen(txn3,3)
                     sumTxnLen  == TL1 + TL2 + TL3
                     Txns == <<GetTxn(txn1),GetTxn(txn2),GetTxn(txn3)>>
                     \* SerializeedTxn is committed txns' in ts order.
                     SerializeedTxn == [i \in 1..sumTxnLen |-> IF i \in 1..TL1 THEN Txns[1][i]
                                                               ELSE IF i \in TL1 + 1 .. TL1 + TL2 THEN Txns[2][i - TL1]
                                                               ELSE Txns[3][i - TL1 - TL2]]
                                                            
                     RL1 == Len(Read_result[txn1]) 
                     RL2 == Len(Read_result[txn2])
                     RL3 == Len(Read_result[txn3])                                     
                     sumReadLen == RL1 + RL2 + RL3 
                     \* SerializedRead is committed txns' Read_result in ts order.
                     SerializedRead == [i \in 1..sumReadLen |-> IF i \in 1..RL1 THEN Read_result[txn1][i]
                                                                 ELSE IF i \in RL1 + 1 .. RL1 + RL2 THEN Read_result[txn2][i - RL1]
                                                                 ELSE Read_result[txn3][i - RL1 - RL2]]
                
                IN  \* You can use the following statement to view an example. 
                    \* /\ Assert(\E i \in 1..3: Record[i].status = "aborted",<<SerializeedTxn,SerializedRead,Len(SerializeedTxn)>>)
                    /\ CheckInv(SerializeedTxn,SerializedRead,Len(SerializeedTxn))
    
           
\* The ts of two txns cannot be equal.    
invTs == ~ \E i,j \in DOMAIN Record : /\ i /= j
                                      /\ Record[i].ts = Record[j].ts
                                      
invOthers == 
          \* Only txns in staging status need to be checked.
          /\ ~ \E i \in DOMAIN Record : /\ Record[i].status /= "staging"
                                        /\ i \in DOMAIN Tocheck
          \* If a txn is committed, all commands of it have been executed.
          /\ \A i \in DOMAIN Record : Record[i].status = "committed" => Txn_exeid[i] = Len(Transactions[i]) + 1 
          \* Without txn committed, MVCCData has no other values.
          /\ ~ /\ \A i \in DOMAIN Record : Record[i].status /= "committed" 
               /\ \E k \in KEYS : Len(MVCCData[k]) /= 1 
          
invType ==  /\ \A i \in DOMAIN Record :  /\ Record[i].status \in {"pending","staging","aborted","committed"} 
                                         /\ Record[i].attempt \in 0..MaxAttempt + 1
                                         /\ Txn_exeid[i] \in 1..Len(Transactions[i]) + 1
            /\ \A i \in DOMAIN Intent_write : /\ i \in KEYS 
                                              /\ Intent_write[i] /= Nil 
                                                     \* Ts in intent write is smaller than system_ts.
                                                  => /\ Intent_write[i].ts < System_ts
                                                     /\ Intent_write[i].txn_id \in 1..3
                                             
                        
TxnInv == /\ invCorrect 
          /\ invTs
          /\ invOthers
          /\ invType


Properties ==
    \* The ts of intent_write increases monotonically.
    /\ [][\A k \in KEYS: \/ Intent_write'[k] = Nil
                         \/ Intent_write[k] = Nil
                         \/ Intent_write'[k].ts >= Intent_write[k].ts]_Intent_write
    \* The ts of each element of tscache increases monotonically.
    /\ [][\A k \in KEYS: Tscache'[k] >= Tscache[k]]_Tscache
    \* System_ts increases monotonically.
    /\ [][System_ts' > System_ts]_System_ts
    \* Finally, the status of all txns is "committed" or "aborted"
    /\ <>(\A i \in DOMAIN Record: \/ Record[i].status = "committed"
                                  \/ Record[i].status = "aborted")
                                  
==================================




