------------------------------ MODULE AmberioTraceReplay ------------------------------
EXTENDS Naturals, Sequences, FiniteSets

\* Deterministic replay of an observed integration trace.
\* The trace format is provided through constant `Observed`.

CONSTANTS
    Paths,
    WriteIds,
    Etags,
    Replicas,
    Leader,
    MinWriteReplicas,
    MaxGeneration,
    Observed

ASSUME Paths # {}
ASSUME WriteIds # {}
ASSUME Etags # {}
ASSUME Replicas # {}
ASSUME Leader \in Replicas
ASSUME MinWriteReplicas \in Nat
ASSUME MaxGeneration \in Nat \ {0}

IntMin(a, b) == IF a <= b THEN a ELSE b
IntMax(a, b) == IF a >= b THEN a ELSE b

WriteQuorum == IntMax(1, IntMin(MinWriteReplicas, Cardinality(Replicas)))
NoneEtag == "none"

HeadKinds == {"none", "meta", "tombstone"}
HeadRecord == [kind: HeadKinds, generation: 0..MaxGeneration, etag: Etags \cup {NoneEtag}]
CacheRecord == [seen: BOOLEAN, generation: 0..MaxGeneration, etag: Etags \cup {NoneEtag}]
OpKinds == {"put", "put_retry", "delete"}

EventRecord ==
    [ kind: OpKinds,
      path: Paths,
      writeId: WriteIds,
      etag: Etags \cup {NoneEtag},
      generation: 0..MaxGeneration,
      committedReplicas: 1..Cardinality(Replicas),
      quorumReached: BOOLEAN,
      fromCache: BOOLEAN ]

EmptyHead == [kind |-> "none", generation |-> 0, etag |-> NoneEtag]
EmptyCache == [seen |-> FALSE, generation |-> 0, etag |-> NoneEtag]

VARIABLES head, cache, index, valid

vars == <<head, cache, index, valid>>

Init ==
    /\ head = [p \in Paths |-> EmptyHead]
    /\ cache = [p \in Paths |-> [w \in WriteIds |-> EmptyCache]]
    /\ index = 0
    /\ valid = TRUE

PutPre(evt) ==
    /\ evt.kind = "put"
    /\ ~evt.fromCache
    /\ evt.etag # NoneEtag
    /\ ~cache[evt.path][evt.writeId].seen
    /\ head[evt.path].generation < MaxGeneration
    /\ evt.generation = head[evt.path].generation + 1
    /\ evt.quorumReached <=> (evt.committedReplicas >= WriteQuorum)

PutApply(evt) ==
    /\ head' = [head EXCEPT ![evt.path] =
            [kind |-> "meta", generation |-> evt.generation, etag |-> evt.etag]]
    /\ cache' =
        IF evt.quorumReached
        THEN [cache EXCEPT ![evt.path][evt.writeId] =
            [seen |-> TRUE, generation |-> evt.generation, etag |-> evt.etag]]
        ELSE cache

PutRetryPre(evt) ==
    /\ evt.kind = "put_retry"
    /\ evt.fromCache
    /\ evt.quorumReached
    /\ evt.committedReplicas >= WriteQuorum
    /\ cache[evt.path][evt.writeId].seen
    /\ evt.generation = cache[evt.path][evt.writeId].generation
    /\ evt.etag = cache[evt.path][evt.writeId].etag

PutRetryApply(evt) ==
    /\ UNCHANGED <<head, cache>>

DeletePre(evt) ==
    /\ evt.kind = "delete"
    /\ ~evt.fromCache
    /\ evt.etag = NoneEtag
    /\ head[evt.path].generation < MaxGeneration
    /\ evt.generation = head[evt.path].generation + 1
    /\ evt.quorumReached
    /\ evt.committedReplicas >= WriteQuorum

DeleteApply(evt) ==
    /\ head' = [head EXCEPT ![evt.path] =
            [kind |-> "tombstone", generation |-> evt.generation, etag |-> NoneEtag]]
    /\ UNCHANGED cache

ReplayStep ==
    /\ index < Len(Observed)
    /\ LET evt == Observed[index + 1]
       IN
           /\ IF valid /\ PutPre(evt)
              THEN
                  /\ PutApply(evt)
                  /\ valid' = TRUE
              ELSE
                  /\ IF valid /\ PutRetryPre(evt)
                     THEN
                         /\ PutRetryApply(evt)
                         /\ valid' = TRUE
                     ELSE
                         /\ IF valid /\ DeletePre(evt)
                            THEN
                                /\ DeleteApply(evt)
                                /\ valid' = TRUE
                            ELSE
                                /\ UNCHANGED <<head, cache>>
                                /\ valid' = FALSE
           /\ index' = index + 1

Done ==
    /\ index = Len(Observed)
    /\ UNCHANGED vars

Next == ReplayStep \/ Done

Spec == Init /\ [][Next]_vars

TypeInvariant ==
    /\ head \in [Paths -> HeadRecord]
    /\ cache \in [Paths -> [WriteIds -> CacheRecord]]
    /\ index \in 0..Len(Observed)
    /\ valid \in BOOLEAN
    /\ Observed \in Seq(EventRecord)

ValidNeverDrops == valid

IndexInBounds == index <= Len(Observed)

CacheEntriesAreMetaWrites ==
    \A p \in Paths :
        \A w \in WriteIds :
            cache[p][w].seen => head[p].generation >= cache[p][w].generation

TraceAccepted == index = Len(Observed) => valid

=============================================================================
