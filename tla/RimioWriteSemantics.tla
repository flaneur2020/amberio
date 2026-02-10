------------------------------ MODULE RimioWriteSemantics ------------------------------
EXTENDS Naturals, FiniteSets, Sequences

\* Abstracts the current external write/delete behavior:
\* - generation increments per path on every non-idempotent PUT/DELETE
\* - PUT idempotency cache keyed by (path, writeId)
\* - quorum controls whether cache is written, not whether local head exists

CONSTANTS
    Paths,
    WriteIds,
    Etags,
    Replicas,
    Leader,
    MinWriteReplicas,
    MaxGeneration

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
Peers == Replicas \ {Leader}
NoneEtag == "none"

HeadKinds == {"none", "meta", "tombstone"}
HeadRecord == [kind: HeadKinds, generation: 0..MaxGeneration, etag: Etags \cup {NoneEtag}]
CacheRecord == [seen: BOOLEAN, generation: 0..MaxGeneration, etag: Etags \cup {NoneEtag}]
OpKinds == {"put", "put_retry", "delete"}
OpRecord ==
    [ kind: OpKinds,
      path: Paths,
      writeId: WriteIds,
      generation: 0..MaxGeneration,
      committedReplicas: 1..Cardinality(Replicas),
      quorumReached: BOOLEAN ]

EmptyHead == [kind |-> "none", generation |-> 0, etag |-> NoneEtag]
EmptyCache == [seen |-> FALSE, generation |-> 0, etag |-> NoneEtag]

VARIABLES head, cache, opLog

vars == <<head, cache, opLog>>

Init ==
    /\ head = [p \in Paths |-> EmptyHead]
    /\ cache = [p \in Paths |-> [w \in WriteIds |-> EmptyCache]]
    /\ opLog = <<>>

PutNew(p, w, e, ackPeers) ==
    /\ p \in Paths
    /\ w \in WriteIds
    /\ e \in Etags
    /\ ackPeers \subseteq Peers
    /\ ~cache[p][w].seen
    /\ head[p].generation < MaxGeneration
    /\ LET newGeneration == head[p].generation + 1
           committedReplicas == 1 + Cardinality(ackPeers)
           quorumReached == committedReplicas >= WriteQuorum
       IN
           /\ head' =
                [head EXCEPT ![p] = [kind |-> "meta", generation |-> newGeneration, etag |-> e]]
           /\ cache' =
                IF quorumReached
                THEN [cache EXCEPT ![p][w] =
                        [seen |-> TRUE, generation |-> newGeneration, etag |-> e]]
                ELSE cache
           /\ opLog' = Append(
                opLog,
                [ kind |-> "put",
                  path |-> p,
                  writeId |-> w,
                  generation |-> newGeneration,
                  committedReplicas |-> committedReplicas,
                  quorumReached |-> quorumReached ]
              )

PutRetry(p, w) ==
    /\ p \in Paths
    /\ w \in WriteIds
    /\ cache[p][w].seen
    /\ UNCHANGED <<head, cache>>
    /\ opLog' = Append(
        opLog,
        [ kind |-> "put_retry",
          path |-> p,
          writeId |-> w,
          generation |-> cache[p][w].generation,
          committedReplicas |-> WriteQuorum,
          quorumReached |-> TRUE ]
      )

Delete(p, w, ackPeers) ==
    /\ p \in Paths
    /\ w \in WriteIds
    /\ ackPeers \subseteq Peers
    /\ head[p].generation < MaxGeneration
    /\ LET newGeneration == head[p].generation + 1
           committedReplicas == 1 + Cardinality(ackPeers)
           quorumReached == committedReplicas >= WriteQuorum
       IN
           /\ head' = [head EXCEPT ![p] =
                [kind |-> "tombstone", generation |-> newGeneration, etag |-> NoneEtag]]
           /\ UNCHANGED cache
           /\ opLog' = Append(
                opLog,
                [ kind |-> "delete",
                  path |-> p,
                  writeId |-> w,
                  generation |-> newGeneration,
                  committedReplicas |-> committedReplicas,
                  quorumReached |-> quorumReached ]
              )

Next ==
    \/ \E p \in Paths, w \in WriteIds, e \in Etags, ackPeers \in SUBSET Peers :
        PutNew(p, w, e, ackPeers)
    \/ \E p \in Paths, w \in WriteIds :
        PutRetry(p, w)
    \/ \E p \in Paths, w \in WriteIds, ackPeers \in SUBSET Peers :
        Delete(p, w, ackPeers)

Spec == Init /\ [][Next]_vars

TypeInvariant ==
    /\ head \in [Paths -> HeadRecord]
    /\ cache \in [Paths -> [WriteIds -> CacheRecord]]
    /\ opLog \in Seq(OpRecord)

HeadWellFormed ==
    \A p \in Paths :
        (head[p].kind = "none") <=> (head[p].generation = 0 /\ head[p].etag = NoneEtag)

CacheEntryConsistent ==
    \A p \in Paths :
        \A w \in WriteIds :
            cache[p][w].seen =>
                (cache[p][w].generation > 0 /\ cache[p][w].etag # NoneEtag)

IsStateChanging(op) == op.kind = "put" \/ op.kind = "delete"

MonotonicHistory ==
    \A p \in Paths :
        \A i \in 1..Len(opLog), j \in 1..Len(opLog) :
            /\ i < j
            /\ IsStateChanging(opLog[i])
            /\ IsStateChanging(opLog[j])
            /\ opLog[i].path = p
            /\ opLog[j].path = p
            => opLog[i].generation < opLog[j].generation

CacheEntriesReflectCommittedWrites ==
    \A p \in Paths :
        \A w \in WriteIds :
            cache[p][w].seen =>
                \E i \in 1..Len(opLog) :
                    /\ opLog[i].kind = "put"
                    /\ opLog[i].path = p
                    /\ opLog[i].writeId = w
                    /\ opLog[i].quorumReached
                    /\ opLog[i].generation = cache[p][w].generation
                    /\ opLog[i].committedReplicas >= WriteQuorum

=============================================================================
