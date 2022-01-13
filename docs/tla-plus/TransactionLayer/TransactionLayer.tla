--------------------------------- MODULE TransactionLayer---------------------------------------------------

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
\* Every txn's timestamp(ts), status, attempt count and command.(transaction record)
VARIABLES Record
\* System time used to timestamp the transaction.
VARIABLES System_ts
\* Every txn's result of Read.
VARIABLES Read_result
\* Index of command which should be executed next. 
VARIABLES Txn_exeid
\* Most recently used ts of each key.
VARIABLES Tscache


\* The following VARIABLES are used to create three txns, and will never be changed.
VARIABLES Transactions
VARIABLES T1,T2,T3
VARIABLES op1,op2,op3,op4,op5,op6,op7,op8,op9,opt,opj,opq
VARIABLES key1,key2,key3,key4,key5,key6,key7,key8,key9,keyt,keyj,keyq
ops  == <<op1,op2,op3,op4,op5,op6,op7,op8,op9,opt,opj,opq>>
keys == <<key1,key2,key3,key4,key5,key6,key7,key8,key9,keyt,keyj,keyq>>
Unchangedvars == <<Transactions,ops,keys,T1,T2,T3>>

\* All VARIABLES
vars == <<Unchangedvars,MVCCData,Record,System_ts,Read_result,Txn_exeid,Intent_write,Tscache>>

\* Keys in command.
KEYS == {"A","B"}

-----------------------------------------------------------------------------------------------------------------------------    
\* Initiate txn1.
\* in this case:
\* op              key        value(useless if Read)
\* Write           A          1
\* Read            A          2(useless)
\* Write or Read   B          3(useless if Read)
\* Txn1 have 2 cases.
InitT1 == /\ op1 \in {Write} /\ op2 \in {Read} /\ op3 \in {Write, Read}
          /\ key1 \in {"A"}  /\ key2 \in {"A"} /\ key3 \in {"B"}
          /\ T1 = <<[op    |-> op1, key   |-> key1, value |-> 1],
                    [op    |-> op2, key   |-> key2, value |-> 2],
                    [op    |-> op3, key   |-> key3, value |-> 3]>>
                  
\* Initiate txn2.
\* in this case:
\* op              key        value(useless if Read)
\* Write           B          4
\* Write or Read   A or B     5(useless if Read)
\* Write or Read   B          6(useless if Read)
\* Read            A          7(useless)      
\* Txn2 have 2*2*2 = 8 cases.                
InitT2 == /\ op4 \in {Write} /\ op5 \in {Write, Read} /\ op6 \in {Write, Read} /\ op7 \in {Read}
          /\ key4 \in {"B"}  /\ key5 \in {"A","B"}    /\ key6 \in {"B"}        /\ key7 \in {"A"}
          /\ T2 = <<[op    |-> op4, key   |-> key4, value |-> 4],
                    [op    |-> op5, key   |-> key5, value |-> 5],
                    [op    |-> op6, key   |-> key6, value |-> 6],
                    [op    |-> op7, key   |-> key7, value |-> 7]>>
\* Initiate txn3.
\* in this case:
\* txn3 have 1 case.  
InitT3 == /\ op8 \in {Read} /\ op9 \in {Write} /\ opt \in  {Read} /\ opj \in {Write} /\ opq \in {Read}
          /\ key8 \in {"A"} /\ key9 \in {"B"}  /\ keyt \in {"B"}  /\ keyj \in {"A"}  /\ keyq \in {"A"}
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

\* For convenience, I separated Intent_write from MVCCData.                 
InitData == /\ MVCCData = [c \in KEYS |-> <<[ts  |-> 0, value  |-> 0]>>]  
            /\ Intent_write = [i \in KEYS |-> Nil] 
    
InitRecoed == Record = <<>>

InitSign == /\ System_ts    = 1
            /\ Txn_exeid    = [i \in 1..3 |-> 1]
            /\ Tscache      = [i \in KEYS |-> 0] 

InitResult == Read_result = [i \in 1..3 |-> <<>>]

Init == /\ InitTransactions
        /\ InitData
        /\ InitRecoed
        /\ InitSign
        /\ InitResult
        
-----------------------------------------------------------------------------------------------------------------------------    
\* Return the minimum value from a set, or undefined if the set is empty.
Min(s) == CHOOSE x \in s : \A y \in s : x <= y
\* Return the maximum value from a set, or undefined if the set is empty.
Max(s) == CHOOSE x \in s : \A y \in s : x >= y

\* Get the value of key in MVCCData according to ts. 
GetLastNum(k,ts) == LET Indexs     == {i \in 1..Len(MVCCData[k]):  MVCCData[k][i].ts <= ts}
                        maxTsIndex == CHOOSE x \in Indexs : \A y \in Indexs : MVCCData[k][x].ts >= MVCCData[k][y].ts
                    IN MVCCData[k][maxTsIndex].value
                    
\* Add something(a) to a tuple(s) on index(i).
SthIndexAdd(s,i,a) == [t \in DOMAIN s \union {i} |-> IF t \in DOMAIN s THEN s[t] ELSE a]

\* Exist newer committed value.
ExistNCV(key,ts) == \/ /\ Intent_write[key] /= Nil
                       /\ Intent_write[key].ts > ts
                       /\ Record[Intent_write[key].tid].status = "committed"
                    \/ /\ Intent_write[key] = Nil
                       /\ MVCCData[key][Len(MVCCData[key])].ts > ts
\* Begin a txn.
BeginTransaction(tid) == 
               LET newT == [status |-> "pending", attempt |-> 0, ts |-> System_ts ,command |-> Transactions[tid]]
               IN 
                  /\ tid \notin DOMAIN Record
                  /\ Record' = SthIndexAdd(Record,tid,newT)
                  /\ System_ts' = System_ts + 1
                  /\ UNCHANGED <<Unchangedvars,MVCCData,Read_result,Txn_exeid,Intent_write,Tscache>>
                  
                  
\* Execute next command of a txn.
PipeLineWrite(tid) == 
           /\ Record[tid].status = "pending"
           /\ Txn_exeid[tid] <= Cardinality(DOMAIN Record[tid].command) 
           /\ LET \* This command.
                  cmd   == Record[tid].command[Txn_exeid[tid]]
                  \* Operation of this command.
                  op    == cmd.op
                  \* Key of this command.
                  key   == cmd.key
                  \* Value of this command.
                  value == cmd.value
                  \* Timestamp of this txn.
                  ts    == Record[tid].ts
                  \* Intent_write on this key 
                  iw    == Intent_write[key]
              IN /\ Assert(op \in {Read, Write}, "wrong op")
                 /\ \/ /\ op = Read
                       \* op is Read
                       /\ \/ /\ iw = Nil
                             \* Without intent_write on key
                             \* Read MVCCData according to ts.
                             /\ Read_result' = [Read_result EXCEPT ![tid] = Append(@,GetLastNum(key,ts))]
                             /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = @ + 1]
                             \* Update ts cache.
                             /\ Tscache' = [Tscache EXCEPT ![key] = Max({ts,@})]
                             /\ UNCHANGED <<MVCCData,Record,System_ts,Intent_write>>
                          \/ /\ iw /= Nil
                             \* With intent_write on key
                             
                             /\ LET \* ID of the conflicting txn in Intent_write.
                                    otid    == iw.tid
                                    \* Timestamp of the conflicting txn in Record.
                                    ots     == Record[otid].ts
                                    \* Timestamp of the conflicting txn in Intent_write.
                                    iwts    == iw.ts
                                    \* Status of the conflicting txn in Record.
                                    ostatus == Record[otid].status
                                    \* Value of the conflicting txn in Intent_write.
                                    iwv     == iw.value
                                    
                                IN /\ \/ /\ iwts > ts
                                         \* A txn with a large ts attempted to write.
                                         \* Ignore it and read MVCCData according to ts.
                                         /\ Read_result' = [Read_result EXCEPT ![tid] = Append(@,GetLastNum(key,ts))]
                                         /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = @ + 1]
                                         /\ Tscache' = [Tscache EXCEPT ![key] = Max({ts,@})]
                                         /\ UNCHANGED <<MVCCData,Record,System_ts,Intent_write>>
                                      \/ /\ iwts = ts
                                         \* This Intent_write was written by myself.
                                         \* Read this Intent_write directly.
                                         /\ Read_result' = [Read_result EXCEPT ![tid] = Append(@,iwv)]
                                         /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = @ + 1]
                                         /\ Tscache' = [Tscache EXCEPT ![key] = Max({ts,@})]
                                         /\ UNCHANGED <<MVCCData,Record,System_ts,Intent_write>>
                                      \/ /\ iwts < ts
                                         \* Judge whether the intent write is written by this txn.
                                         /\ \/ /\ otid = tid
                                               \* Due to retry of this txn.
                                               \* Delete this Intent_write and read MVCCData according to ts.
                                               /\ Intent_write' = [Intent_write EXCEPT ![key] = Nil]
                                               /\ Read_result' = [Read_result EXCEPT ![tid] = Append(@,GetLastNum(key,ts))]
                                               /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = @ + 1]
                                               /\ Tscache' = [Tscache EXCEPT ![key] = Max({ts,@})]
                                               /\ UNCHANGED <<MVCCData,Record,System_ts>>
                                            \/ /\ otid /= tid
                                               \* This is a txn conflict.
                                               \* Handle this conflict according to status.
                                               /\ Assert(ostatus \in {"committed","aborted","staging","pending"},"wrong status")
                                               /\ \/ /\ ostatus = "committed"
                                                     \* Persist this Intent_write and read MVCCData according to ts.
                                                     /\ Read_result' = [Read_result EXCEPT ![tid] = Append(@,iwv)]
                                                     /\ Intent_write' = [Intent_write EXCEPT ![key] = Nil]
                                                     /\ MVCCData' = [MVCCData EXCEPT ![key] = Append(MVCCData[key],[ts    |-> iwts,
                                                                                                                    value |-> iwv])]
                                                     /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = @ + 1]
                                                     /\ Tscache' = [Tscache EXCEPT ![key] = Max({ts,@})]
                                                     /\ UNCHANGED <<Record,System_ts>>
                                                     
                                                  \/ /\ ostatus = "aborted"
                                                     \* Clean this Intent_write and read MVCCData according to ts.
                                                     /\ Read_result' = [Read_result EXCEPT ![tid] = Append(@,GetLastNum(key,ts))]
                                                     /\ Intent_write' = [Intent_write EXCEPT ![key] = Nil]
                                                     /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = @ + 1]
                                                     /\ Tscache' = [Tscache EXCEPT ![key] = Max({ts,@})]
                                                     
                                                     /\ UNCHANGED <<MVCCData,Record,System_ts>>
                                                  \/ /\ ostatus = "staging"
                                                     /\ FALSE
                                                  \/ /\ ostatus = "pending"
                                                     \* Compare priority.
                                                     /\ \/ \* Push ts of conflicting txn,and retry it.
                                                           /\ Record' = [Record EXCEPT ![otid] = [ts      |-> System_ts,
                                                                                                 \* Set status according to attempt.
                                                                                                  status  |-> IF @.attempt < MaxAttempt THEN "pending" ELSE "aborted",
                                                                                                  attempt |-> @.attempt + 1,
                                                                                                  command |-> @.command]]
                                                           /\ System_ts' = System_ts + 1
                                                           /\ Read_result' = [Read_result EXCEPT ![tid] = Append(@,GetLastNum(key,ts)), ![otid] = <<>>]
                                                           /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = @ + 1,![otid] = 1]
                                                           /\ Tscache' = [Tscache EXCEPT ![key] = Max({ts,@})]
                                                           
                                                        \/ \* Retry myself.
                                                           /\ Record' = [Record EXCEPT ![tid] = [ts      |-> System_ts,
                                                                                                 \* Set status according to attempt.
                                                                                                 status  |-> IF @.attempt < MaxAttempt THEN "pending" ELSE "aborted",
                                                                                                 attempt |-> @.attempt + 1,
                                                                                                 command |-> @.command]]
                                                           /\ System_ts' = System_ts + 1
                                                           /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = 1]
                                                           /\ Read_result' = [Read_result EXCEPT ![tid] = <<>>]
                                                           /\ UNCHANGED <<Tscache>>
                                                     /\ UNCHANGED <<MVCCData,Intent_write>>
                    \/ /\ op = Write
                       \* op is Write
                       /\ \/ /\ ts < Tscache[key] 
                             \* Txn with large ts have already read this key.
                             \* Retry myself.
                             /\ Record' = [Record EXCEPT ![tid] = [ts      |-> System_ts,
                                                                   \* Set status according to attempt.
                                                                   status  |-> IF @.attempt < MaxAttempt THEN "pending" ELSE "aborted",
                                                                   attempt |-> @.attempt + 1,
                                                                   command |-> @.command]]
                             /\ System_ts' = System_ts + 1                                      
                             /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = 1]
                             
                             /\ Read_result' = [Read_result EXCEPT ![tid] = <<>>]
                             /\ UNCHANGED <<Unchangedvars,MVCCData,Intent_write,Tscache>>
                          \/ /\ ts >= Tscache[key]
                             /\ \/ /\ ExistNCV(key,ts)
                                   \* Exist newer committed value.
                                   \* Retry myself.
                                   /\ Record' = [Record EXCEPT ![tid] = [ts      |-> System_ts,
                                                                         \* Set status according to attempt.
                                                                         status  |-> IF @.attempt < MaxAttempt THEN "pending" ELSE "aborted",
                                                                         attempt |-> @.attempt + 1,
                                                                         command |-> @.command]]
                                                                   
                                   /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = 1]
                                   /\ Read_result' = [Read_result EXCEPT ![tid] = <<>>]
                                   /\ System_ts' = System_ts + 1
                                   
                                   /\ UNCHANGED<<MVCCData,Intent_write,Tscache>>
                                \/ /\ ~ExistNCV(key,ts)
                                   \* Do not exist newer committed value.
                                   /\ \/ /\ iw = Nil 
                                         \* Without Intent_write on this key.
                                         /\ Intent_write' = [Intent_write EXCEPT ![key] = [tid   |-> tid,
                                                                                           value |-> value,
                                                                                           ts    |-> ts]]
                                         /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = @ + 1]
                                         /\ UNCHANGED <<MVCCData,Record,System_ts,Read_result,Tscache>>
                                      \/ /\ iw /= Nil
                                         \* Compare priority
                                         /\ LET otid    == iw.tid
                                                ots     == Record[otid].ts
                                                ostatus == Record[otid].status
                                                iwts    == iw.ts
                                                iwv     == iw.value
                                            IN /\ \/ /\ otid = tid
                                                     \* This Intent_write was written by myself.
                                                     \* Ignore it and update Intent_write.
                                                     /\ Intent_write' = [Intent_write EXCEPT ![key] = [tid   |-> tid,
                                                                                                       value |-> value,
                                                                                                       ts    |-> ts]]
                                                     /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = @ + 1]
                                                     /\ UNCHANGED <<MVCCData,Record,System_ts,Read_result,Tscache>>
                                                  \/ /\ otid /= tid 
                                                     \* This is a txn conflict.
                                                     \* Handle this conflict according to status.
                                                     /\ \/ /\ ostatus = "committed"
                                                           \* Persist this Intent_write and update Intent_write.
                                                           /\ Intent_write' = [Intent_write EXCEPT ![key] = [tid   |-> tid,
                                                                                                             value |-> value,
                                                                                                             ts    |-> ts]]
                                                           /\ MVCCData' = [MVCCData EXCEPT ![key] = Append(MVCCData[key],[ts    |-> iwts,
                                                                                                                          value |-> iwv])]
                                                                                                                          
                                                           /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = @ + 1]
                                                           /\ UNCHANGED <<Record,System_ts,Read_result,Tscache>> 
                                                        \/ /\ ostatus = "aborted"
                                                           \* Update Intent_write.
                                                           /\ Intent_write' = [Intent_write EXCEPT ![key] = [tid   |-> tid,
                                                                                                             value |-> value,
                                                                                                             ts    |-> ts]]
                                                           /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = @ + 1]
                                                           /\ UNCHANGED <<MVCCData,Record,System_ts,Read_result,Tscache>> 
                                                        \/ /\ ostatus = "staging"
                                                           /\ FALSE
                                                        \/ /\ ostatus = "pending"
                                                           \* Compare priority.
                                                           /\ \/ \* Abort the conflicting transaction.
                                                                 /\ Record' = [Record EXCEPT ![otid].status = "aborted"]
                                                                 /\ Read_result' = [Read_result EXCEPT ![otid] = <<>>]
                                                                 /\ UNCHANGED <<MVCCData,System_ts,Txn_exeid,Intent_write,Tscache>> 
                                                              \/ \* Retry myself
                                                                 /\ Record' = [Record EXCEPT ![tid] = [ts      |-> System_ts,
                                                                                                       \* Set status according to attempt.
                                                                                                       status  |-> IF @.attempt < MaxAttempt THEN "pending" ELSE "aborted",
                                                                                                       attempt |-> @.attempt + 1,
                                                                                                       command |-> @.command]]
                                                                 /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = 1]
                                                                 /\ Read_result' = [Read_result EXCEPT ![tid] = <<>>]
                                                                 /\ System_ts' = System_ts + 1
                                                                 /\ UNCHANGED <<MVCCData,Intent_write,Tscache>>
                                                 
                 /\ UNCHANGED <<Unchangedvars>>

\* Start to commit a txn.               
StartParallelCommit(tid) == 
            /\ Record' = [Record EXCEPT ![tid].status = "staging"]
            /\ UNCHANGED <<Unchangedvars,MVCCData,System_ts,Read_result,Txn_exeid,Intent_write,Tscache>>

\* Check whether all inflight write have been persisted. 
CheckInflightAndCommit(tid) ==
            /\ \/ \* If all inflight write have been persisted.
                  /\ Record' = [Record EXCEPT ![tid].status = "committed"]
                  /\ UNCHANGED <<System_ts,Read_result,Txn_exeid>>
                  
               \* For convenience, we assume that all inflight writes will be persisted. 
               \* You can remove (**) below to add failure scenarios.
               (*\/ \* If some inflight write fails persistence
                  /\ Record' = [Record EXCEPT ![tid] = [ts      |-> System_ts,
                                                        \* Set status according to attempt.
                                                        status  |-> IF @.attempt < MaxAttempt THEN "pending" ELSE "aborted",
                                                        attempt |-> @.attempt + 1,
                                                        command |-> @.command]]
                  /\ Txn_exeid' = [Txn_exeid EXCEPT ![tid] = 1]
                  /\ Read_result' = [Read_result EXCEPT ![tid] = <<>>]
                  /\ System_ts' = System_ts + 1 *)
            /\ UNCHANGED <<Unchangedvars,MVCCData,Intent_write,Tscache>>
            
CommitTransaction(tid) == 
        \/ /\ Record[tid].status = "pending" 
           \* All commands have been executed.
           /\ Txn_exeid[tid] = Cardinality(DOMAIN Record[tid].command) + 1
           /\ StartParallelCommit(tid)
        \/ /\ Record[tid].status = "staging"
           /\ CheckInflightAndCommit(tid)

\* Move the value from intent write to MVCCData.
CleanIntentWrite(k) == 
         LET tid == Intent_write[k].tid
         IN /\ Intent_write[k] /= Nil
            /\ Record[tid].status = "committed"
            /\ Intent_write' = [Intent_write EXCEPT ![k] = Nil]
            /\ MVCCData' = [MVCCData EXCEPT ![k] = Append(@,[ts    |-> Intent_write[k].ts,
                                                             value |-> Intent_write[k].value])]
            /\ UNCHANGED <<Unchangedvars,Record,System_ts,Read_result,Txn_exeid,Tscache>>                                                                 


Next == 
        \/ \E tid \in 1..3 : BeginTransaction(tid)
        \/ \E tid \in DOMAIN Record : PipeLineWrite(tid)
        \/ \E tid \in DOMAIN Record : CommitTransaction(tid)
        \/ \E k \in KEYS : CleanIntentWrite(k)

   
-----------------------------------------------------------------------------------------------------------------------------     

                                        

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
                
RECURSIVE GetSerializedTxn(_,_)
GetSerializedTxn(Result,Set) ==
       IF {id \in Set : Record[id].status = "committed"} /= {}
       THEN LET nextTxnid == CHOOSE x \in Set : \A y \in Set : /\ Record[x].status = "committed"
                                                               /\ Record[y].status = "committed" => Record[x].ts <= Record[y].ts
            IN GetSerializedTxn(Result \o Record[nextTxnid].command, Set \ {nextTxnid})
       ELSE Result
       
RECURSIVE GetSerializedRead(_,_)
GetSerializedRead(Result,Set) ==
       IF {id \in Set : Record[id].status = "committed"} /= {}
       THEN LET nextTxnid == CHOOSE x \in Set : \A y \in Set : /\ Record[x].status = "committed"
                                                               /\ Record[y].status = "committed" => Record[x].ts <= Record[y].ts
            IN GetSerializedRead(Result \o Read_result[nextTxnid], Set \ {nextTxnid})
       ELSE Result
       
\* verify its correctness.      
invCorrect ==\* When all txns are terminated and all intent write is written to MVCCData, check its correctness.
             /\ Cardinality({id \in DOMAIN Record : Record[id].status = "committed"}) +
                Cardinality({id \in DOMAIN Record : Record[id].status = "aborted"}) = 3
             /\ \A k \in KEYS : Intent_write[k] = Nil 
           =>   LET  SerializedTxn == GetSerializedTxn(<<>>,1..3)                                             
                     SerializedRead == GetSerializedRead(<<>>,1..3)
                     num == Len(SerializedTxn) 
                IN  
                \* To verify its correctness, we verify:
                \* 1.The read value is the most recently written value.
                \* 2.The last written value determines the current value of MVCCData. 
                 /\ \A index \in 1..num : LET Item == SerializedTxn[index]
                                          IN \/ /\ Item.op = Write
                                                /\ (~ \E i \in (index+1)..num : /\ SerializedTxn[i].op  = Write
                                                                                /\ SerializedTxn[i].key = Item.key)
                                                   => GetLastNum(Item.key,999) = Item.value            
                                             \/ /\ Item.op = Read
                                                /\ LET samekeyW    == {i \in 1..(index-1) : /\ SerializedTxn[i].op = Write
                                                                                            /\ SerializedTxn[i].key = Item.key}
                                                       beforeRnum  == Cardinality({i \in 1..(index-1) : SerializedTxn[i].op = Read})
                                                       thisR       == SerializedRead[beforeRnum + 1]
                                                             
                                                   IN \* Without written before, read 0.
                                                      \/ /\ samekeyW = {}
                                                         /\ thisR = 0
                                                      \* With written before, read recently written value.
                                                      \/ /\ samekeyW /= {}
                                                         /\ thisR = SerializedTxn[CHOOSE x \in samekeyW : \A y \in samekeyW : x >= y].value
           
\* The ts of two txns cannot be equal.    
invTs == ~ \E i,j \in DOMAIN Record : /\ i /= j
                                      /\ Record[i].ts = Record[j].ts
 
invType ==  /\ \A i \in DOMAIN Record :  /\ Record[i].status \in {"pending","staging","aborted","committed"} 
                                         /\ Record[i].attempt \in 0..MaxAttempt + 1
                                         /\ Txn_exeid[i] \in 1..Len(Transactions[i]) + 1
            /\ \A i \in DOMAIN Intent_write : /\ i \in KEYS 
                                              /\ Intent_write[i] /= Nil 
                                                     \* Ts in intent write is smaller than system_ts.
                                                  => /\ Intent_write[i].ts < System_ts
                                                     /\ Intent_write[i].tid \in 1..3
                                             
invMVCCData == \* Data must be written by comitted txn. 
             /\ Cardinality({id \in DOMAIN Record : Record[id].status = "committed"}) +
                Cardinality({id \in DOMAIN Record : Record[id].status = "aborted"}) = 3
             /\ \A k \in KEYS : Intent_write[k] = Nil 
           => LET comSet == {i \in 1..3 : Record[i].status = "committed"}
                  tsSet  == {Record[i].ts : i \in comSet} \union {0}
                  MVCCDataSet == [k \in KEYS |-> {MVCCData[k][i].ts : i \in 1..Len(MVCCData[k])}]
              IN  \A k \in KEYS : \A ele \in MVCCDataSet[k] : ele \in tsSet 
                 
           
invOthers == 
          \* If a txn is committed, all commands of it have been executed.
          /\ \A i \in DOMAIN Record : Record[i].status = "committed" => Txn_exeid[i] = Len(Transactions[i]) + 1 
          \* Without txn committed, MVCCData has no other values.
          /\ ~ /\ \A i \in DOMAIN Record : Record[i].status /= "committed" 
               /\ \E k \in KEYS : Len(MVCCData[k]) /= 1
          
                               
TxnInv == /\ invCorrect 
          /\ invTs
          /\ invType
          /\ invMVCCData
          /\ invOthers
          
-----------------------------------------------------------------------------------------------------------------------------          
\* In a txn, when reads a value, the value must be the same as the value of the most recent operation on the key.
\* On the other word : If most recent operation in same txn is Read, two result of read should be equal.
\* If most recent operation in same txn is Write, the read operation should read the written value.
ReadProperty == 
       \A i \in 1..3: (Len(Read_result'[i]) > Len(Read_result[i]))
                         =>    LET txn == Transactions[i]
                                   Rr  == Read_result'[i]
                                   
                                   ReadIndexSet == {c \in 1..Len(txn) : /\ txn[c].op = Read
                                                                        /\ Cardinality({t \in 1..c-1 : txn[t].op = Read}) = Len(Rr) - 1}
                                   
                                   ReadIndex == CHOOSE c \in ReadIndexSet: TRUE
                                   ReadKey == txn[ReadIndex].key
                                   ReadValue == txn[ReadIndex].value
                                    
                                   
                                   sKeySet == {c \in 1..ReadIndex - 1: txn[c].key = ReadKey}
                                   cmdIndex == Max(sKeySet)
                                   cmd   == txn[cmdIndex]
                                   
                                   
                                   thisReadResult == Rr[Len(Rr)]  
                                   LastReadResultIndex == Cardinality({c \in 1..cmdIndex: txn[c].op = Read})
                                   
                                IN 
                                   /\ Assert(Cardinality(ReadIndexSet) = 1 ,ReadIndexSet)
                                   /\ \/ /\ Cardinality(sKeySet) > 0 
                                         /\ \/ /\ cmd.op = Write
                                               /\ Assert(cmd.value = thisReadResult,"do not equal")
                                            \/ /\ cmd.op = Read 
                                               /\ Assert(Rr[LastReadResultIndex] = thisReadResult,"do not equal")
                                      \/ /\ Cardinality(sKeySet) = 0
                                         /\ (thisReadResult /= 0) =>
                                             \/ /\ thisReadResult \in {1,2,3}
                                                /\ Record[1].status = "committed"
                                             \/ /\ thisReadResult \in {4,5,6,7}
                                                /\ Record[2].status = "committed"
                                             \/ /\ thisReadResult \in {8,9,10,11}
                                                /\ Record[3].status = "committed"
                                          
\* After the Write operation, the content of the Write_intent will be modified accordingly                              
WriteProperty == 
    \A i \in 1..3 : (/\ Txn_exeid'[i] > Txn_exeid[i]
                     /\ Transactions[i][Txn_exeid[i]].op = Write)
                  => 
                     LET cmd == Transactions[i][Txn_exeid[i]]
                         key == cmd.key
                         value == cmd.value
                         ts == Record[i].ts
                     IN Intent_write'[key] = [value |-> value,
                                              ts    |-> ts,
                                              tid   |-> i]
                        


Properties ==
    /\ [][WriteProperty]_Txn_exeid
    /\ [][ReadProperty]_Read_result
    \* The ts of each element of tscache increases monotonically.
    /\ [][\A k \in KEYS: Tscache'[k] >= Tscache[k]]_Tscache
    \* System_ts increases monotonically.
    /\ [][System_ts' > System_ts]_System_ts
    /\ <>[](/\ \A i \in DOMAIN Record : Record[i].status \in {"committed","aborted"}
            /\ Cardinality(DOMAIN Record) = 3)
                
==============================================================================================================================




