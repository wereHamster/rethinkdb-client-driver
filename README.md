A Haskell client driver for RethinkDB

It differs from the other driver ([rethinkdb][rethinkdb-haskell]) in that it
uses advanced Haskell magic to add proper types to the terms. In particular,
the term used in the query and its result type are connected with each other.

Example:

```haskell
-- An expression which yields the number one.
one :: Exp Double
one = constant 1

-- An expression which yields a reference to the test table. Tables can be
-- used as input into other functions. Or they are converted into a 'Sequence'
-- when used as the top-level query expression.
testTable :: Exp Table
testTable = table "test"

main :: IO ()
main = do
    h <- newHandle

    -- Here Haskell knows that the type of res is 'Either Error Double',
    -- because the expression was of type 'Double'
    res1 <- run h one

    -- Table is converted to a 'Sequence Datum' on the client. This means the
    -- type of 'res2' is correctly inferred to be 'Either Error (Sequence Datum)'.
    res2 <- run h testTable
```


[rethinkdb-haskell]: https://hackage.haskell.org/package/rethinkdb
