{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts  #-}

module Database.RethinkDB.Terms where


import           Data.Text (Text)
import qualified Data.HashMap.Strict as HMS
import           Database.RethinkDB.Types



listDatabases :: Exp (Array Text)
listDatabases = ListDatabases

createDatabase :: Exp Text -> Exp Object
createDatabase = CreateDatabase

dropDatabase :: Exp Text -> Exp Object
dropDatabase = DropDatabase


listTables :: Exp Database -> Exp (Array Text)
listTables = ListTables

createTable :: Exp Database -> Exp Text -> Exp Object
createTable = CreateTable

dropTable :: Exp Database -> Exp Text -> Exp Object
dropTable = DropTable


db :: Exp Text -> Exp Database
db = Database


table :: Exp Text -> Exp Table
table = Table


getField :: (IsObject o) => Exp o -> Exp Text -> Exp Datum
getField = GetField

extractField :: (IsSequence s) => Exp s -> Exp Text -> Exp (Sequence Datum)
extractField = GetField


get :: Exp Table -> Exp Text -> Exp SingleSelection
get = Get


coerceTo :: (Any v, Any r) => Exp v -> Exp Text => Exp r
coerceTo = Coerce


getAll :: (IsDatum a) => Exp Table -> [Exp a] -> Exp (Array Datum)
getAll = GetAll

getAllIndexed :: (IsDatum a) => Exp Table -> [Exp a] -> Text -> Exp (Sequence Datum)
getAllIndexed = GetAllIndexed


add :: [Exp Double] -> Exp Double
add = Add


insert :: Exp Table -> Object -> Exp Object
insert tbl obj = Insert tbl obj emptyOptions


upsert :: Exp Table -> Object -> Exp Object
upsert tbl obj = Insert tbl obj (HMS.singleton "upsert" (Bool True))


delete :: (Any a) => Exp a -> Exp Object
delete = Delete


limit :: (Any a) => Exp (Sequence a) -> Exp Double -> Exp (Sequence a)
limit = Take


append :: (Any a) => Exp (Array a) -> Exp a -> Exp (Array a)
append = Append


prepend :: (Any a) => Exp (Array a) -> Exp a -> Exp (Array a)
prepend = Prepend


filter :: (Any a, Any f) => Exp (Sequence a) -> Exp f -> Exp (Sequence a)
filter = Filter


isEmpty :: (IsSequence a) => Exp a -> Exp Bool
isEmpty = IsEmpty


eq :: (Any a, Any b) => Exp a -> Exp b -> Exp Bool
eq = Eq


keys :: (IsObject a) => Exp a -> Exp (Array Text)
keys = Keys
