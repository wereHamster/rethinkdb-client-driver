{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE TypeFamilies          #-}

module Database.RethinkDB.Types where


import           Control.Applicative
import           Control.Monad.State (State, gets, modify, evalState)

import           Data.Word
import           Data.String
import           Data.Text           (Text)
import           Data.Time
import           Data.Time.Clock.POSIX

import           Data.Aeson          (FromJSON, parseJSON, toJSON)
import           Data.Aeson.Types    (Parser, Value)
import qualified Data.Aeson          as A

import           Data.Vector         (Vector)
import qualified Data.Vector         as V
import           Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HMS

import           Database.RethinkDB.Types.Datum



------------------------------------------------------------------------------
-- | A Term is a JSON expression which can be sent to the server. Building a
-- term is a stateful operation, so the whole process happens inside a 'State'
-- monad.

class Term a where
    toTerm :: a -> State Context A.Value

instance Term A.Value where
    toTerm = pure



------------------------------------------------------------------------------
-- | Building a RethinkDB query from an expression is a stateful process, and
-- is done using this as the context.

data Context = Context
    { varCounter :: !Int
      -- ^ How many 'Var's have been allocated. See 'newVar'.

    , defaultDatabase :: !(Exp Database)
      -- ^ The default database for the case that the 'Table' expression
      -- doesn't specify one.
    }


compileTerm :: Exp Database -> State Context A.Value -> A.Value
compileTerm db e = evalState e (Context 0 db)


-- | Allocate a new var index from the context.
newVar :: State Context Int
newVar = do
    ix <- gets varCounter
    modify $ \s -> s { varCounter = ix + 1 }
    pure ix



class IsDatum a

instance IsDatum Datum

instance Term Datum where
    toTerm (Null    ) = pure $ A.Null
    toTerm (Bool   x) = toTerm x
    toTerm (Number x) = toTerm x
    toTerm (String x) = toTerm x
    toTerm (Array  x) = toTerm x
    toTerm (Object x) = toTerm x
    toTerm (Time   x) = toTerm x

instance FromResponse Datum where
    parseResponse = responseAtomParser

instance FromResponse (Maybe Datum) where
    parseResponse r = case (responseType r, V.toList (responseResult r)) of
        (SuccessAtom, [a]) -> do
            res0 <- parseWire a
            case res0 of
                Null -> pure Nothing
                res  -> pure $ Just res
        _                  -> fail $ "responseAtomParser: Not a single-element vector " ++ show (responseResult r)



------------------------------------------------------------------------------
-- | For a boolean type, we're reusing the standard Haskell 'Bool' type.

instance IsDatum Bool

instance FromResponse Bool where
    parseResponse = responseAtomParser

instance Term Bool where
    toTerm = pure . A.Bool



------------------------------------------------------------------------------
-- | Numbers are 'Double' (unlike 'Aeson', which uses 'Scientific'). No
-- particular reason.

instance IsDatum Double

instance FromResponse Double where
    parseResponse = responseAtomParser

instance Term Double where
    toTerm = pure . toJSON


instance FromResponse Int where
    parseResponse = responseAtomParser


instance FromResponse Char where
    parseResponse = responseAtomParser

instance FromResponse [Char] where
    parseResponse = responseAtomParser



------------------------------------------------------------------------------
-- | For strings, we're using the Haskell 'Text' type.

instance IsDatum Text

instance FromResponse Text where
    parseResponse = responseAtomParser

instance Term Text where
    toTerm = pure . toJSON



------------------------------------------------------------------------------
-- | Arrays are vectors of 'Datum'.

instance (IsDatum a) => IsDatum    (Array a)
instance (IsDatum a) => IsSequence (Array a)

instance (FromDatum a) => FromResponse (Array a) where
    parseResponse = responseAtomParser

instance (Term a) => Term (Array a) where
    toTerm v = do
        vals    <- mapM toTerm (V.toList v)
        options <- toTerm emptyOptions
        pure $ A.Array $ V.fromList $
            [ A.Number 2
            , toJSON vals
            , toJSON $ options
            ]



------------------------------------------------------------------------------
-- | Objects are maps from 'Text' to 'Datum'. Like 'Aeson', we're using
-- 'HashMap'.

class (IsDatum a) => IsObject a


instance IsDatum  Object
instance IsObject Object

instance FromResponse Object where
    parseResponse = responseAtomParser

instance Term Object where
    toTerm x = do
        items <- mapM (\(k, v) -> (,) <$> pure k <*> toTerm v) $ HMS.toList x
        pure $ A.Object $ HMS.fromList $ items



------------------------------------------------------------------------------
-- | Time in RethinkDB is represented similar to the 'ZonedTime' type. Except
-- that the JSON representation on the wire looks different from the default
-- used by 'Aeson'. Therefore we have a custom 'FromRSON' and 'ToRSON'
-- instances.

instance IsDatum  ZonedTime
instance IsObject ZonedTime

instance FromResponse ZonedTime where
    parseResponse = responseAtomParser

instance Term ZonedTime where
    toTerm x = pure $ A.object
        [ "$reql_type$" A..= ("TIME" :: Text)
        , "timezone"    A..= (timeZoneOffsetString $ zonedTimeZone x)
        , "epoch_time"  A..= (realToFrac $ utcTimeToPOSIXSeconds $ zonedTimeToUTC x :: Double)
        ]



------------------------------------------------------------------------------
-- UTCTime

instance IsDatum  UTCTime
instance IsObject UTCTime

instance FromResponse UTCTime where
    parseResponse = responseAtomParser

instance Term UTCTime where
    toTerm = toTerm . utcToZonedTime utc



------------------------------------------------------------------------------
-- | Tables are something you can select objects from.
--
-- This type is not exported, and merely serves as a sort of phantom type. On
-- the client tables are converted to a 'Sequence'.

data Table = MkTable

instance IsSequence Table



------------------------------------------------------------------------------
-- | 'SingleSelection' is essentially a 'Maybe Object', where 'Nothing' is
-- represented with 'Null' in the network protocol.

data SingleSelection = SingleSelection
    deriving (Show)

instance IsDatum  SingleSelection
instance IsObject SingleSelection



------------------------------------------------------------------------------
-- | A 'Database' is something which contains tables. It is a server-only
-- type.

data Database = MkDatabase



------------------------------------------------------------------------------
-- | Bounds are used in 'Between'.

data Bound = Open !Datum | Closed !Datum

boundDatum :: Bound -> Datum
boundDatum (Open   x) = x
boundDatum (Closed x) = x

boundString :: Bound -> Text
boundString (Open   _) = "open"
boundString (Closed _) = "closed"



------------------------------------------------------------------------------
-- | ConflictResolutionStrategy
--
-- How conflicts should be resolved.

data ConflictResolutionStrategy

    = CRError
      -- ^ Do not insert the new document and record the conflict as an error.
      -- This is the default.

    | CRReplace
      -- ^ Replace the old document in its entirety with the new one.

    | CRUpdate
      -- ^ Update fields of the old document with fields from the new one.


instance ToDatum ConflictResolutionStrategy where
    toDatum CRError   = String "error"
    toDatum CRReplace = String "replace"
    toDatum CRUpdate  = String "update"



------------------------------------------------------------------------------
-- | Used in 'OrderBy'.

data Order = Ascending !Text | Descending !Text

instance Term Order where
    toTerm (Ascending  key) = simpleTerm 73 [SomeExp $ lift key]
    toTerm (Descending key) = simpleTerm 74 [SomeExp $ lift key]



------------------------------------------------------------------------------
-- | Sequences are a bounded list of items. The server may split the sequence
-- into multiple chunks when sending it to the client. When the response is
-- a partial sequence, the client may request additional chunks until it gets
-- a 'Done'.

data Sequence a
    = Done    !(Vector a)
    | Partial !Token !(Vector a)


class IsSequence a


instance Show (Sequence a) where
    show (Done      v) = "Done " ++ (show $ V.length v)
    show (Partial _ v) = "Partial " ++ (show $ V.length v)

instance (FromDatum a) => FromResponse (Sequence a) where
    parseResponse = responseSequenceParser

instance IsSequence (Sequence a)

instance (FromDatum a) => FromDatum (Sequence a) where
    parseDatum (Array x) = Done <$> V.mapM parseDatum x
    parseDatum _         = fail "Sequence"



------------------------------------------------------------------------------

data Exp a where
    Constant :: (ToDatum a) => a -> Exp a
    -- Any object which can be converted to RSON can be treated as a constant.
    -- Furthermore, many basic Haskell types have a 'Lift' instance which turns
    -- their values into constants.


    MkArray :: [Exp a] -> Exp (Array a)
    -- Create an array from a list of expressions. This is an internal function,
    -- you should use 'lift' instead.


    --------------------------------------------------------------------------
    -- Database administration

    ListDatabases  :: Exp (Array Text)
    CreateDatabase :: Exp Text -> Exp Object
    DropDatabase   :: Exp Text -> Exp Object
    WaitDatabase   :: Exp Database -> Exp Object


    --------------------------------------------------------------------------
    -- Table administration

    ListTables     :: Exp Database -> Exp (Array Text)
    CreateTable    :: Exp Database -> Exp Text -> Exp Object
    DropTable      :: Exp Database -> Exp Text -> Exp Object
    WaitTable      :: Exp Table -> Exp Object


    --------------------------------------------------------------------------
    -- Index administration

    ListIndices    :: Exp Table -> Exp (Array Text)

    CreateIndex    :: (IsDatum a) => Exp Table -> Exp Text -> (Exp Object -> Exp a) -> Exp Object
    -- Create a new secondary index on the table. The index has a name and a
    -- projection function which is applied to every object which is added to the table.

    DropIndex      :: Exp Table -> Exp Text -> Exp Object
    IndexStatus    :: Exp Table -> [Exp Text] -> Exp (Array Object)
    WaitIndex      :: Exp Table -> [Exp Text] -> Exp Object


    Database       :: Exp Text -> Exp Database
    Table          :: Maybe (Exp Database) -> Exp Text -> Exp Table

    Coerce         :: Exp a -> Exp Text -> Exp b
    Eq             :: (IsDatum a, IsDatum b) => Exp a -> Exp b -> Exp Bool
    Ne             :: (IsDatum a, IsDatum b) => Exp a -> Exp b -> Exp Bool
    Not            :: Exp Bool -> Exp Bool

    Match :: Exp Text -> Exp Text -> Exp Datum
    -- First arg is the string, second a regular expression.

    Get            :: Exp Table -> Exp Text -> Exp SingleSelection
    GetAll         :: (IsDatum a) => Exp Table -> [Exp a] -> Exp (Array Datum)
    GetAllIndexed  :: (IsDatum a) => Exp Table -> [Exp a] -> Text -> Exp (Sequence Datum)

    Add            :: (Num a) => [Exp a] -> Exp a
    Multiply       :: (Num a) => [Exp a] -> Exp a

    All :: [Exp Bool] -> Exp Bool
    -- True if all the elements in the input are True.

    Any :: [Exp Bool] -> Exp Bool
    -- True if any element in the input is True.

    GetField :: (IsObject a, IsDatum r) => Exp Text -> Exp a -> Exp r
    -- Get a particular field from an object (or SingleSelection).

    HasFields :: (IsObject a) => [Text] -> Exp a -> Exp Bool
    -- True if the object has all the given fields.

    Take           :: (IsSequence s) => Exp Double -> Exp s -> Exp s
    Append         :: (IsDatum a) => Exp (Array a) -> Exp a -> Exp (Array a)
    Prepend        :: (IsDatum a) => Exp (Array a) -> Exp a -> Exp (Array a)
    IsEmpty        :: (IsSequence a) => Exp a -> Exp Bool
    Delete         :: Exp a -> Exp Object

    InsertObject   :: ConflictResolutionStrategy -> Exp Table -> Object -> Exp Object
    -- Insert a single object into the table.

    InsertSequence :: (IsSequence s) => Exp Table -> Exp s -> Exp Object
    -- Insert a sequence into the table.

    Filter :: (IsSequence s) => (Exp a -> Exp Bool) -> Exp s -> Exp s
    Map :: (IsSequence s) => (Exp a -> Exp b) -> Exp s -> Exp (Sequence b)

    Between :: (IsSequence s) => (Bound, Bound) -> Exp s -> Exp s
    -- Select all elements whose primary key is between the two bounds.

    BetweenIndexed :: (IsSequence s) => Text -> (Bound, Bound) -> Exp s -> Exp s
    -- Select all elements whose secondary index is between the two bounds.

    OrderBy :: (IsSequence s) => [Order] -> Exp s -> Exp (Array Datum)
    -- Order a sequence based on the given order specificiation.

    OrderByIndexed :: (IsSequence s) => Order -> Exp s -> Exp (Array Datum)
    -- Like OrderBy but uses a secondary index instead of a object field.

    Keys :: (IsObject a) => Exp a -> Exp (Array Text)

    Var :: Int -> Exp a
    -- A 'Var' is used as a placeholder in input to functions.

    Function :: State Context ([Int], Exp a) -> Exp f
    -- Creates a function. The action should take care of allocating an
    -- appropriate number of variables from the context. Note that you should
    -- not use this constructor directly. There are 'Lift' instances for all
    -- commonly used functions.

    Call :: Exp f -> [SomeExp] -> Exp r
    -- Call the given function. The function should take the same number of
    -- arguments as there are provided.

    Limit :: (IsSequence s) => Double -> Exp s -> Exp s
    -- Limit the number of items in the sequence.

    Nth :: (IsSequence s, IsDatum r) => Double -> Exp s -> Exp r
    -- Return the n-th element in the sequence.

    UUID :: Exp Text
    -- An expression which when evaluated will generate a fresh UUID (in its
    -- standard string encoding).

    Now :: Exp ZonedTime
    -- The time when the query was received by the server.

    Timezone :: Exp ZonedTime -> Exp Text
    -- The timezone in which the given time is.

    RandomInteger :: Exp Int -> Exp Int -> Exp Int
    -- Takes a lower and upper bound and returns a random integer between
    -- the two. Note that the lower bound is closed, the upper bound is open,
    -- ie: [min, max)

    RandomFloat :: Exp Double -> Exp Double -> Exp Double
    -- Same as 'RandomInteger' but uses floating-point numbers.

    Info :: Exp a -> Exp Object
    -- Gets info about anything.

    Default :: Exp a -> Exp a -> Exp a
    -- Evaluate the first argument. If it throws an error then the second
    -- argument is returned.

    Error :: Exp Text -> Exp a
    -- Throw an error with the given message.


instance Term (Exp a) where
    toTerm (Constant datum) =
        toTerm $ toDatum datum

    toTerm (MkArray xs) =
        simpleTerm 2 (map SomeExp xs)


    toTerm ListDatabases =
        noargTerm 59

    toTerm (CreateDatabase name) =
        simpleTerm 57 [SomeExp name]

    toTerm (DropDatabase name) =
        simpleTerm 58 [SomeExp name]

    toTerm (WaitDatabase db) =
        simpleTerm 177 [SomeExp db]


    toTerm (ListTables db) =
        simpleTerm 62 [SomeExp db]

    toTerm (CreateTable db name) =
        simpleTerm 60 [SomeExp db, SomeExp name]

    toTerm (DropTable db name) =
        simpleTerm 61 [SomeExp db, SomeExp name]

    toTerm (WaitTable table) =
        simpleTerm 177 [SomeExp table]


    toTerm (ListIndices table) =
        simpleTerm 77 [SomeExp table]

    toTerm (CreateIndex table name f) =
        simpleTerm 75 [SomeExp table, SomeExp name, SomeExp (lift f)]

    toTerm (DropIndex table name) =
        simpleTerm 76 [SomeExp table, SomeExp name]

    toTerm (IndexStatus table indices) =
        simpleTerm 139 ([SomeExp table] ++ map SomeExp indices)

    toTerm (WaitIndex table indices) =
        simpleTerm 140 ([SomeExp table] ++ map SomeExp indices)


    toTerm (Database name) =
        simpleTerm 14 [SomeExp name]

    toTerm (Table mbDatabase name) = do
        db <- maybe (gets defaultDatabase) pure mbDatabase
        simpleTerm 15 [SomeExp db, SomeExp name]

    toTerm (Filter f s) =
        simpleTerm 39 [SomeExp s, SomeExp (lift f)]

    toTerm (Map f s) =
        simpleTerm 38 [SomeExp s, SomeExp (lift f)]

    toTerm (Between (l, u) s) =
        termWithOptions 36 [SomeExp s, SomeExp $ lift (boundDatum l), SomeExp $ lift (boundDatum u)] $
            HMS.fromList
                [ ("left_bound",  toJSON $ String (boundString l))
                , ("right_bound", toJSON $ String (boundString u))
                ]

    toTerm (BetweenIndexed index (l, u) s) =
        termWithOptions 36 [SomeExp s, SomeExp $ lift (boundDatum l), SomeExp $ lift (boundDatum u)] $
            HMS.fromList
                [ ("left_bound",  toJSON $ String (boundString l))
                , ("right_bound", toJSON $ String (boundString u))
                , ("index",       toJSON $ String index)
                ]

    toTerm (OrderBy spec s) = do
        s'    <- toTerm s
        spec' <- mapM toTerm spec
        simpleTerm 41 ([s'] ++ spec')

    toTerm (OrderByIndexed spec s) = do
        s'    <- toTerm s
        spec' <- toTerm spec
        termWithOptions 41 [s'] $ HMS.singleton "index" spec'

    toTerm (InsertObject crs table obj) =
        termWithOptions 56 [SomeExp table, SomeExp (lift obj)] $
            HMS.singleton "conflict" (toJSON $ toDatum crs)

    toTerm (InsertSequence table s) =
        termWithOptions 56 [SomeExp table, SomeExp s] HMS.empty

    toTerm (Delete selection) =
        simpleTerm 54 [SomeExp selection]

    toTerm (GetField field obj) =
        simpleTerm 31 [SomeExp obj, SomeExp field]

    toTerm (HasFields fields obj) =
        simpleTerm 32 ([SomeExp obj] ++ map (SomeExp . lift) fields)

    toTerm (Coerce value typeName) =
        simpleTerm 51 [SomeExp value, SomeExp typeName]

    toTerm (Add values) =
        simpleTerm 24 (map SomeExp values)

    toTerm (Multiply values) =
        simpleTerm 26 (map SomeExp values)

    toTerm (All values) =
        simpleTerm 67 (map SomeExp values)

    toTerm (Any values) =
        simpleTerm 66 (map SomeExp values)

    toTerm (Eq a b) =
        simpleTerm 17 [SomeExp a, SomeExp b]

    toTerm (Ne a b) =
        simpleTerm 18 [SomeExp a, SomeExp b]

    toTerm (Not e) =
        simpleTerm 23 [SomeExp e]

    toTerm (Match str re) =
        simpleTerm 97 [SomeExp str, SomeExp re]

    toTerm (Get table key) =
        simpleTerm 16 [SomeExp table, SomeExp key]

    toTerm (GetAll table keys) =
        simpleTerm 78 ([SomeExp table] ++ map SomeExp keys)

    toTerm (GetAllIndexed table keys index) =
        termWithOptions 78 ([SomeExp table] ++ map SomeExp keys)
            (HMS.singleton "index" (toJSON $ String index))

    toTerm (Take n s) =
        simpleTerm 71 [SomeExp s, SomeExp n]

    toTerm (Append array value) =
        simpleTerm 29 [SomeExp array, SomeExp value]

    toTerm (Prepend array value) =
        simpleTerm 80 [SomeExp array, SomeExp value]

    toTerm (IsEmpty s) =
        simpleTerm 86 [SomeExp s]

    toTerm (Keys a) =
        simpleTerm 94 [SomeExp a]

    toTerm (Var a) =
        simpleTerm 10 [SomeExp $ lift $ (fromIntegral a :: Double)]

    toTerm (Function a) = do
        (vars, f) <- a
        simpleTerm 69 [SomeExp $ Constant $ V.fromList $ map (Number . fromIntegral) vars, SomeExp f]

    toTerm (Call f args) =
        simpleTerm 64 ([SomeExp f] ++ args)

    toTerm (Limit n s) =
        simpleTerm 71 [SomeExp s, SomeExp (lift n)]

    toTerm (Nth n s) =
        simpleTerm 45 [SomeExp s, SomeExp (lift n)]

    toTerm UUID =
        noargTerm 169

    toTerm Now =
        noargTerm 103

    toTerm (Timezone time) =
        simpleTerm 127 [SomeExp time]

    toTerm (RandomInteger lo hi) =
        simpleTerm 151 [SomeExp lo, SomeExp hi]

    toTerm (RandomFloat lo hi) =
        termWithOptions 151 [SomeExp lo, SomeExp hi] $
            HMS.singleton "float" (toJSON $ Bool True)

    toTerm (Info a) =
        simpleTerm 79 [SomeExp a]

    toTerm (Default action def) =
        simpleTerm 92 [SomeExp action, SomeExp def]

    toTerm (Error message) =
        simpleTerm 12 [SomeExp message]


noargTerm :: Int -> State Context A.Value
noargTerm termType = pure $ A.Array $ V.fromList [toJSON termType]

simpleTerm :: (Term a) => Int -> [a] -> State Context A.Value
simpleTerm termType args = do
    args' <- mapM toTerm args
    pure $ A.Array $ V.fromList [toJSON termType, toJSON args']

termWithOptions :: (Term a) => Int -> [a] -> HashMap Text Value -> State Context A.Value
termWithOptions termType args options = do
    args' <- mapM toTerm args
    pure $ A.Array $ V.fromList [toJSON termType, toJSON args', toJSON options]


-- | Convenience to for automatically converting a 'Text' to a constant
-- expression.
instance IsString (Exp Text) where
    fromString = lift . (fromString :: String -> Text)


instance Num (Exp Double) where
    fromInteger = Constant . fromInteger

    a + b = Add [a, b]
    a * b = Multiply [a, b]

    abs _    = error "Num (Exp a): abs not implemented"
    signum _ = error "Num (Exp a): signum not implemented"
    negate _ = error "Num (Exp a): negate not implemented"



------------------------------------------------------------------------------
-- | The class of types e which can be lifted into c. All basic Haskell types
-- which can be represented as 'Exp' are instances of this, as well as certain
-- types of functions (unary and binary).

class Lift c e where
    -- | Type-level function which simplifies the type of @e@ once it is lifted
    -- into @c@. This is used for functions where we strip the signature so
    -- that we don't have to define dummy 'Term' instances for those.
    type Simplified e

    lift :: e -> c (Simplified e)


instance Lift Exp Bool where
    type Simplified Bool = Bool
    lift = Constant

instance Lift Exp Int where
    type Simplified Int = Int
    lift = Constant

instance Lift Exp Double where
    type Simplified Double = Double
    lift = Constant

instance Lift Exp Char where
    type Simplified Char = Char
    lift = Constant

instance Lift Exp String where
    type Simplified String = String
    lift = Constant

instance Lift Exp Text where
    type Simplified Text = Text
    lift = Constant

instance Lift Exp Object where
    type Simplified Object = Object
    lift = Constant

instance Lift Exp Datum where
    type Simplified Datum = Datum
    lift = Constant

instance Lift Exp ZonedTime where
    type Simplified ZonedTime = ZonedTime
    lift = Constant

instance Lift Exp UTCTime where
    type Simplified UTCTime = ZonedTime
    lift = Constant . utcToZonedTime utc

instance Lift Exp (Array Datum) where
    type Simplified (Array Datum) = (Array Datum)
    lift = Constant

instance Lift Exp [Exp a] where
    type Simplified [Exp a] = Array a
    lift = MkArray

instance Lift Exp (Exp a -> Exp r) where
    type Simplified (Exp a -> Exp r) = Exp r
    lift f = Function $ do
        v1 <- newVar
        pure $ ([v1], f (Var v1))

instance Lift Exp (Exp a -> Exp b -> Exp r) where
    type Simplified (Exp a -> Exp b -> Exp r) = Exp r
    lift f = Function $ do
        v1 <- newVar
        v2 <- newVar
        pure $ ([v1, v2], f (Var v1) (Var v2))



------------------------------------------------------------------------------
-- 'call1', 'call2' etc generate a function call expression. These should be
-- used instead of the 'Call' constructor because they provide type safety.

-- | Call an unary function with the given argument.
call1 :: (Exp a -> Exp r) -> Exp a -> Exp r
call1 f a = Call (lift f) [SomeExp a]


-- | Call an binary function with the given arguments.
call2 :: (Exp a -> Exp b -> Exp r) -> Exp a -> Exp b -> Exp r
call2 f a b = Call (lift f) [SomeExp a, SomeExp b]


emptyOptions :: Object
emptyOptions = HMS.empty



------------------------------------------------------------------------------
-- | Because the arguments to functions are polymorphic (the individual
-- arguments can, and often have, different types).

data SomeExp where
     SomeExp :: Exp a -> SomeExp

instance Term SomeExp where
    toTerm (SomeExp e) = toTerm e



------------------------------------------------------------------------------
-- | The type of result you get when executing a query of 'Exp a'.
type family Result a

type instance Result Text            = Text
type instance Result Double          = Double
type instance Result Int             = Int
type instance Result Char            = Char
type instance Result String          = String
type instance Result Bool            = Bool
type instance Result ZonedTime       = ZonedTime

type instance Result Table           = Sequence Datum
type instance Result Datum           = Datum
type instance Result Object          = Object
type instance Result (Array a)       = Array a
type instance Result SingleSelection = Maybe Datum
type instance Result (Sequence a)    = Sequence a



------------------------------------------------------------------------------
-- | The result of a query. It is either an error or a result (which depends
-- on the type of the query expression). This type is named to be symmetrical
-- to 'Exp', so we get this nice type for 'run'.
--
-- > run :: Handle -> Exp a -> IO (Res a)

type Res a = Either Error (Result a)



------------------------------------------------------------------------------
-- | A value which can be converted from a 'Response'. All types which are
-- defined as being a 'Result a' should have a 'FromResponse a'. Because,
-- uhm.. you really want to be able to extract the result from the response.
--
-- There are two parsers defined here, one for atoms and the other for
-- sequences. These are the only two implementations of parseResponse which
-- should be used.

class FromResponse a where
    parseResponse :: Response -> Parser a


responseAtomParser :: (FromDatum a) => Response -> Parser a
responseAtomParser r = case (responseType r, V.toList (responseResult r)) of
    (SuccessAtom, [a]) -> parseWire a >>= parseDatum
    _                  -> fail $ "responseAtomParser: Not a single-element vector " ++ show (responseResult r)

responseSequenceParser :: (FromDatum a) => Response -> Parser (Sequence a)
responseSequenceParser r = case responseType r of
    SuccessAtom     -> Done    <$> responseAtomParser r
    SuccessSequence -> Done    <$> values
    SuccessPartial  -> Partial <$> pure (responseToken r) <*> values
    rt              -> fail $ "responseSequenceParser: Unexpected type " ++ show rt
  where
    values = V.mapM (\x -> parseWire x >>= parseDatum) (responseResult r)



------------------------------------------------------------------------------
-- | A token is used to refer to queries and the corresponding responses. This
-- driver uses a monotonically increasing counter.

type Token = Word64



data ResponseType
    = SuccessAtom | SuccessSequence | SuccessPartial | SuccessFeed
    | WaitComplete
    | ClientErrorType | CompileErrorType | RuntimeErrorType
    deriving (Show, Eq)


instance FromJSON ResponseType where
    parseJSON (A.Number  1) = pure SuccessAtom
    parseJSON (A.Number  2) = pure SuccessSequence
    parseJSON (A.Number  3) = pure SuccessPartial
    parseJSON (A.Number  4) = pure WaitComplete
    parseJSON (A.Number  5) = pure SuccessFeed
    parseJSON (A.Number 16) = pure ClientErrorType
    parseJSON (A.Number 17) = pure CompileErrorType
    parseJSON (A.Number 18) = pure RuntimeErrorType
    parseJSON _           = fail "ResponseType"



data Response = Response
    { responseToken     :: !Token
    , responseType      :: !ResponseType
    , responseResult    :: !(Vector Value)
    --, responseBacktrace :: ()
    --, responseProfile   :: ()
    } deriving (Show, Eq)



responseParser :: Token -> Value -> Parser Response
responseParser token (A.Object o) =
    Response <$> pure token <*> o A..: "t" <*> o A..: "r"
responseParser _     _          =
    fail "Response: Unexpected JSON value"



------------------------------------------------------------------------------
-- | Errors include a plain-text description which includes further details.
-- The RethinkDB protocol also includes a backtrace which we currently don't
-- parse.

data Error

    = ProtocolError !Text
      -- ^ An error on the protocol level. Perhaps the socket was closed
      -- unexpectedly, or the server sent a message which the driver could not
      -- parse.

    | ClientError !Text
      -- ^ Means the client is buggy. An example is if the client sends
      -- a malformed protobuf, or tries to send [CONTINUE] for an unknown
      -- token.

    | CompileError !Text
      -- ^ Means the query failed during parsing or type checking. For example,
      -- if you pass too many arguments to a function.

    | RuntimeError !Text
      -- ^ Means the query failed at runtime. An example is if you add
      -- together two values from a table, but they turn out at runtime to be
      -- booleans rather than numbers.

    deriving (Eq, Show)
