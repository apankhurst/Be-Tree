# Derange Queries

## General Form 
`derange(R,Filter,Map,Fold,k)`

where 
`R` is the range of keys to query
`Filter` is the function to select which keys to apply `Map` to
`Map` is a function that is applied to the values of the keys that pass `Filter`
`Fold` is a function that defines how the results of the query are aggregated
`k` is a key where the results of the query are accumumater

## More Formal Definitions
### `R`
