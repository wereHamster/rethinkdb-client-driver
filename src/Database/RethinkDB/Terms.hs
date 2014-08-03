{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts  #-}

module Database.RethinkDB.Terms where


import           Data.Text (Text)
import qualified Data.HashMap.Strict as HMS
import           Database.RethinkDB.Types



db :: Exp Text -> Exp Database
db name = Term DB [SomeExp name] emptyOptions


table :: Exp Text -> Exp Table
table name = Term TABLE [SomeExp name] emptyOptions


getField :: (IsObject o) => Exp o -> Exp Text -> Exp Datum
getField obj k = Term GET_FIELD [SomeExp obj, SomeExp k] emptyOptions


extractField :: (IsSequence s, IsDatum a) => Exp s -> Exp Text -> Exp (Sequence a)
extractField s k = Term GET_FIELD [SomeExp s, SomeExp k] emptyOptions


get :: Exp Table -> Exp Text -> Exp SingleSelection
get tbl key =
    Term GET [SomeExp tbl, SomeExp key] emptyOptions


coerceTo :: (Any v) => Exp v -> Exp Text => Exp Text
coerceTo value typeName =
    Term COERCE_TO [SomeExp value, SomeExp typeName] emptyOptions


getAll :: (IsDatum a) => Exp Table -> [Exp a] -> Maybe Text -> Exp Array
getAll tbl keys mbIndex =
    Term GET_ALL ([SomeExp tbl] ++ map SomeExp keys) options
  where
    options = case mbIndex of
        Nothing    -> emptyOptions
        Just index -> HMS.singleton "index" (String index)

getAllIndexed :: (IsDatum a) => Exp Table -> [Exp a] -> Text -> Exp (Sequence Datum)
getAllIndexed tbl keys index =
    Term GET_ALL ([SomeExp tbl] ++ map SomeExp keys) options
  where
    options = HMS.singleton "index" (String index)


add :: [Exp Double] -> Exp Double
add xs = Term ADD (map SomeExp xs) emptyOptions


insert :: Exp Table -> Object -> Exp Object
insert tbl obj = Term INSERT [SomeExp tbl, SomeExp (constant obj)] emptyOptions


upsert :: Exp Table -> Object -> Exp Object
upsert tbl obj = Term INSERT [SomeExp tbl, SomeExp (constant obj)] (HMS.singleton "upsert"(Bool True))


delete :: (Any a) => Exp a -> Exp Object
delete s = Term DELETE [SomeExp s] emptyOptions


limit :: (Any a) => Exp a -> Exp Double -> Exp Table
limit s n = Term LIMIT [SomeExp s, SomeExp n] emptyOptions


append :: Exp Array -> Exp Datum -> Exp Array
append a d = Term APPEND [SomeExp a, SomeExp d] emptyOptions


filter :: (Any a, Any f) => Exp (Sequence a) -> Exp f -> Exp (Sequence a)
filter s f = Term FILTER [SomeExp s, SomeExp f] emptyOptions


isEmpty :: (Any a) => Exp (Sequence a) -> Exp Bool
isEmpty s = Term IS_EMPTY [SomeExp s] emptyOptions


eq :: (Any a, Any b) => Exp a -> Exp b -> Exp Bool
eq a b = Term EQ_ [SomeExp a, SomeExp b] emptyOptions
