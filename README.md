# Haskell client driver for RethinkDB

It differs from the other driver ([rethinkdb][rethinkdb-haskell]) in that it
uses advanced Haskell magic to properly type the terms, queries and responses.


# Structure and usage

The library exposes a single module, `Database.RethinkDB`. There should be
very little which can conflict with other code, so you should be able to
import it unqualified.

To be able to run expressions on the server, you first have to create
a handle, this you can do with `newHandle`. Currently it always connects to
"localhost" and the default RethinkDB client driver port.

Expressions have the type `Exp a`, where the `a` denotes the result you would
get when you run the expression. You can use `lift` to lift many basic Haskell
types (Double, Text, Bool) and certain functions (unary and binary) into
RethinkDB expressions.

RethinkDB uses JSON for encoding on the protocol level, but certain types (eg.
time) have non-standard encoding. This is why the driver uses a separate type
class (`FromRSON` / `ToRSON`) to describe types which can be sent over the
wire.


# Examples

Add two numbers, one and two. Here we lift the addition function and its
arguments into `Exp`, and then use `call2` to call it.

```haskell
h <- newHandle
res <- run h $ call2 (lift (+)) (lift 1) (lift 2)
print res
-- Should print 'Right 3.0'
```

Get all objects in a table.

```haskell
h <- newHandle
Right sequence <- run h $ Table "test"
objs <- collect h sequence
-- objs is now a 'Vector' of all objects in the test table.
```


[rethinkdb-haskell]: https://hackage.haskell.org/package/rethinkdb
