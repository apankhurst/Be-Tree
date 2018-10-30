# Derange Queries

## General Form 
`derange(R,Filter,Map,Fold,k)`

where 
`R` is the range of keys to query
`Filter` is the function to select which keys to apply `Map` to
`Map` is a function that is applied to the values of the keys that pass `Filter`
`Fold` is a function that defines how the results of the query are aggregated
`k` is a key where the results of the query are accumumater

## Formal Definitions
Let `Keys` be the set of all possible keys and `Values` be the set of all possible values

### `R`

* `R` = `(k1,k2) s.t. k1 < k2 and k1, k2 are in Keys`. This range should be 

### `Filter`

* `Filter: Keys -> Boolean`

### `Map`

* `Map: Values x Parametes -> Values`

### `Fold`

* `Fold: Values -> Values`

### `k`

* `k is in `Keys`

## Mechanics

The idea with a derange query is that we want to get the I/O amortization of inserts/deletes in the B^e-tree, while still being able to answer questions about the data in the tree. The entire query is encoded as a message, which as it stands may be too inflexible to support derange queries. Regardless, the range is simply just two keys, a lower bound and an upper bound. To implement any one of the functions above, the B^e-tree needs to store the actual data/location of the function so that it can be appropriately called when it is time for the function to be used.

There are a few possible ways that the derange query can actually behave as it moves through the tree.

1) Delegate responsibility to the node
	- Let the nodes in which derange queries exist handle everything.
	- This means that when a derange query is flushed to a node, the node attempts to apply the derange query to every message that falls within query's range.
	- The node will iterate over the key range, finding all the most up to date messages with keys that are accepted by the filter. If the message is an insert 
then we know that the data we're seeing is the most up-to-date. Apply, the derange query to that insert and split the derange query in to two such that the range of the first is `[query.lower, insert.upper)` and the range of the second is `(insert.upper, query.upper]`. An application is simply defined as applying `Map` to the value of the message and then inserting an upsert message at either the root of the tree or the current node, if the key of the upsert falls in the range of keys covered by the node. The number of upserts can be reduced to just one if we hold off on creating the upsert messages to insert until after all of the queries have been applied to the appropriate inserts. If we aggreagate the results of each `Map` with the `Fold` function, we can use a single upsert as opposed to the same number as there are inserts in the node. 
	- If we ever run into a delete as the most recent message, all that we must do is split the derange query with the key of that message as the midpoint of the split. 
	- The node currently handles the application of all messages. If an insert or delete enters the node, then any message that came before the insert/delete is removed from the node (and tree) and the only message for that key is the insert/delete. Derange quereies however, are not subject to this behavior. A derange query must be answered and with the most recenent information, thus it cannot be removed from the tree until it has reached the key it is destined for. If an update enters the node, the node must check if the most recent message for the key is an insert or delete. If it is an insert, the value of the insert message can be updated and the update deleted. If it is a delete, then update can be deleted as there is no data for that key.  
	- The implications of the above mean that updates and derange queries cause the buffers of nodes to fill, whereas inserts and deletes will cause them to empty. What this may imply is that it is actually more efficient for a derange query to issue an insert message for the accumulation key when it is first issued and then each each subsequent derange query that is applied just updates that key with the result of the map. The downside to using this scheme however is that multiple derange queries cannot accumulate at the same key. As it stands I am unsure as to whether or not this is a negative or positive (two queries can be simultaneously update the same key, are threre use cases for this?). The need to first issue an insert could be mitigated if upserts/updates are composable, meaning that one message could be applied to another and the results of applying the new message to the key it is is destined for is simply equivalent to applying both of those functions to the value at the destination, in order. For example if we have two update messages destined for the same key, one that updates the value by 5, the other that updates the value by -2, we could combine these messages into a single message that just updates the value of the key by 3.  
