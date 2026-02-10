------------------------------ MODULE RimioReplicaSelection ------------------------------
EXTENDS Naturals, Sequences, FiniteSets

\* Models the current replica chooser in `resolve_replica_nodes`:
\* - sort nodes by node_id first (already done by current_nodes)
\* - rotate by slot_id % N
\* - take min(3, N), but at least 1 when N > 0

CONSTANTS
    Slots,
    Nodes

ASSUME Slots # {}
ASSUME Nodes # {}

NodeCount == Len(Nodes)
ReplicaCount == IF NodeCount = 0 THEN 0 ELSE IF NodeCount < 3 THEN NodeCount ELSE 3

NthSeq(seq, i) == seq[i]

Rotate(nodes, start) ==
    [i \in 1..Len(nodes) |-> NthSeq(nodes, ((start + i - 2) % Len(nodes)) + 1)]

PickReplicas(slot) ==
    IF NodeCount = 0
    THEN <<>>
    ELSE
        LET start == (slot % NodeCount) + 1
            rotated == Rotate(Nodes, start)
        IN SubSeq(rotated, 1, ReplicaCount)

VARIABLE replicasBySlot

vars == <<replicasBySlot>>

Init ==
    /\ replicasBySlot = [s \in Slots |-> <<>>]

Compute ==
    /\ replicasBySlot' = [s \in Slots |-> PickReplicas(s)]

Next == Compute /\ UNCHANGED <<>>

Spec == Init /\ [][Next]_vars

TypeInvariant ==
    /\ replicasBySlot \in [Slots -> Seq(Nodes)]

ReplicaCountInvariant ==
    \A s \in Slots :
        Len(replicasBySlot[s]) = ReplicaCount

NoDuplicatesPerSlot ==
    \A s \in Slots :
        \A i \in 1..Len(replicasBySlot[s]), j \in 1..Len(replicasBySlot[s]) :
            (i # j) => replicasBySlot[s][i] # replicasBySlot[s][j]

AllSelectedFromNodes ==
    \A s \in Slots :
        \A i \in 1..Len(replicasBySlot[s]) :
            replicasBySlot[s][i] \in Nodes

=============================================================================
