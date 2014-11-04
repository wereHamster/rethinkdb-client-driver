{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Database.RethinkDB.Types where


import           Control.Applicative
import           Control.Monad
import           Control.Monad.State (State, gets, modify, evalState)

import           Data.Function
import           Data.Word
import           Data.String
import           Data.Text           (Text)
import           Data.Time
import           System.Locale       (defaultTimeLocale)
import           Data.Time.Clock.POSIX

import           Data.Aeson          ((.:), (.=), FromJSON, parseJSON, toJSON)
import           Data.Aeson.Types    (Parser, Value)
import qualified Data.Aeson          as A

import           Data.Vector         (Vector)
import qualified Data.Vector         as V
import           Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HMS

import           GHC.Generics



------------------------------------------------------------------------------
-- | A Term is a JSON expression which can be sent to the server. Building a
-- term is a stateful operation, so the whole process happens inside a 'State'
-- monad.

class Term a where
    toTerm :: a -> State Context A.Value

instance Term A.Value where
    toTerm = return



------------------------------------------------------------------------------
-- | A class describing a type which can be converted to the RethinkDB-specific
-- wire protocol. It is based on JSON, but certain types use a presumably more
-- efficient encoding.

class FromRSON a where
    parseRSON :: A.Value -> Parser a

------------------------------------------------------------------------------
-- | See 'FromRSON'.

class ToRSON a where
    toRSON :: a -> A.Value



------------------------------------------------------------------------------
-- | Building a RethinkDB query from an expression is a stateful process, and
-- is done using this as the context.

data Context = Context
    { varCounter :: Int
      -- ^ How many 'Var's have been allocated. See 'newVar'.
    }


compileTerm :: State Context A.Value -> A.Value
compileTerm e = evalState e (Context 0)


-- | Allocate a new var index from the context.
newVar :: State Context Int
newVar = do
    ix <- gets varCounter
    modify $ \s -> s { varCounter = ix + 1 }
    return ix



------------------------------------------------------------------------------
-- | A sumtype covering all the primitive types which can appear in queries
-- or responses.
--
-- It is similar to the aeson 'Value' type, except that RethinkDB has a few
-- more types (like 'Time'), which have a special encoding in JSON.

data Datum
    = Null
    | Bool   !Bool
    | Number !Double
    | String !Text
    | Array  !(Array Datum)
    | Object !Object
    | Time   !ZonedTime
    deriving (Show, Generic)


class (Term a) => IsDatum a


instance IsDatum Datum

-- | We can't automatically derive 'Eq' because 'ZonedTime' does not have an
-- instance of 'Eq'. See the 'eqTime' function for why we can compare times.
instance Eq Datum where
    (Null    ) == (Null    ) = True
    (Bool   x) == (Bool   y) = x == y
    (Number x) == (Number y) = x == y
    (String x) == (String y) = x == y
    (Array  x) == (Array  y) = x == y
    (Object x) == (Object y) = x == y
    (Time   x) == (Time   y) = x `eqTime` y
    _          == _          = False

instance ToRSON Datum where
    toRSON (Null    ) = A.Null
    toRSON (Bool   x) = toRSON x
    toRSON (Number x) = toRSON x
    toRSON (String x) = toRSON x
    toRSON (Array  x) = toRSON x
    toRSON (Object x) = toRSON x
    toRSON (Time   x) = toRSON x

instance FromRSON Datum where
    parseRSON   (A.Null    ) = pure Null
    parseRSON   (A.Bool   x) = pure $ Bool x
    parseRSON   (A.Number x) = pure $ Number (realToFrac x)
    parseRSON   (A.String x) = pure $ String x
    parseRSON   (A.Array  x) = Array <$> V.mapM parseRSON x
    parseRSON a@(A.Object x) = (Time <$> parseRSON a) <|> do
        -- HashMap does not provide a mapM, what a shame :(
        items <- mapM (\(k, v) -> (,) <$> pure k <*> parseRSON v) $ HMS.toList x
        return $ Object $ HMS.fromList items

instance Term Datum where
    toTerm = return . toRSON

instance FromResponse Datum where
    parseResponse = responseAtomParser


instance FromResponse (Maybe Datum) where
    parseResponse r = case (responseType r, V.toList (responseResult r)) of
        (SuccessAtom, [a]) -> do
            res0 <- parseRSON a
            case res0 of
                Null -> return Nothing
                res  -> return $ Just res
        _                  -> fail $ "responseAtomParser: Not a single-element vector " ++ show (responseResult r)




------------------------------------------------------------------------------
-- | For a boolean type, we're reusing the standard Haskell 'Bool' type.

instance IsDatum Bool

instance FromResponse Bool where
    parseResponse = responseAtomParser

instance FromRSON Bool where
    parseRSON = parseJSON

instance ToRSON Bool where
    toRSON = toJSON

instance Term Bool where
    toTerm = return . toRSON



------------------------------------------------------------------------------
-- | Numbers are 'Double' (unlike 'Aeson', which uses 'Scientific'). No
-- particular reason.

instance IsDatum Double

instance FromResponse Double where
    parseResponse = responseAtomParser

instance FromRSON Double where
    parseRSON = parseJSON

instance ToRSON Double where
    toRSON = toJSON

instance Term Double where
    toTerm = return . toRSON



------------------------------------------------------------------------------
-- | For strings, we're using the Haskell 'Text' type.

instance IsDatum Text

instance FromResponse Text where
    parseResponse = responseAtomParser

instance FromRSON Text where
    parseRSON = parseJSON

instance ToRSON Text where
    toRSON = toJSON

instance Term Text where
    toTerm = return . toRSON



------------------------------------------------------------------------------
-- | Arrays are vectors of 'Datum'.

type Array a = Vector a

instance (IsDatum a) => IsDatum    (Array a)
instance (IsDatum a) => IsSequence (Array a)

instance (FromRSON a) => FromResponse (Array a) where
    parseResponse = responseAtomParser

-- Arrays are encoded as a term MAKE_ARRAY (2).
instance (ToRSON a) => ToRSON (Array a) where
    toRSON v = A.Array $ V.fromList $
        [ A.Number 2
        , toJSON $ map toRSON (V.toList v)
        , toJSON $ toRSON emptyOptions
        ]

instance (FromRSON a) => FromRSON (Array a) where
    parseRSON (A.Array v) = V.mapM parseRSON v
    parseRSON _           = fail "Array"

instance (Term a) => Term (Array a) where
    toTerm v = do
        vals    <- mapM toTerm (V.toList v)
        options <- toTerm emptyOptions
        return $ A.Array $ V.fromList $
            [ A.Number 2
            , toJSON vals
            , toJSON $ options
            ]



------------------------------------------------------------------------------
-- | Objects are maps from 'Text' to 'Datum'. Like 'Aeson', we're using
-- 'HashMap'.

type Object = HashMap Text Datum


class (Term a, IsDatum a) => IsObject a


instance IsDatum  Object
instance IsObject Object

instance FromResponse Object where
    parseResponse = responseAtomParser

instance FromRSON Object where
    parseRSON (A.Object o) = do
        -- HashMap does not provide a mapM, what a shame :(
        items <- mapM (\(k, v) -> (,) <$> pure k <*> parseRSON v) $ HMS.toList o
        return $ HMS.fromList items

    parseRSON _            = fail "Object"

instance ToRSON Object where
    toRSON = A.Object . HMS.map toRSON

instance Term Object where
    toTerm = return . toRSON



------------------------------------------------------------------------------
-- | Time in RethinkDB is represented similar to the 'ZonedTime' type. Except
-- that the JSON representation on the wire looks different from the default
-- used by 'Aeson'. Therefore we have a custom 'FromRSON' and 'ToRSON'
-- instances.

instance IsDatum  ZonedTime
instance IsObject ZonedTime

instance FromResponse ZonedTime where
    parseResponse = responseAtomParser

instance ToRSON ZonedTime where
    toRSON t = A.object
        [ "$reql_type$" .= ("TIME" :: Text)
        , "timezone"    .= (timeZoneOffsetString $ zonedTimeZone t)
        , "epoch_time"  .= (realToFrac $ utcTimeToPOSIXSeconds $ zonedTimeToUTC t :: Double)
        ]

instance FromRSON ZonedTime where
    parseRSON (A.Object o) = do
        reqlType <- o .: "$reql_type$"
        guard $ reqlType == ("TIME" :: Text)

        -- Parse the timezone using 'parseTime'. This overapproximates the
        -- possible responses from the server, but better than rolling our
        -- own timezone parser.
        tz <- o .: "timezone" >>= \tz -> case parseTime defaultTimeLocale "%Z" tz of
            Just d -> pure d
            _      -> fail "Could not parse TimeZone"

        t <- o .: "epoch_time" :: Parser Double
        return $ utcToZonedTime tz $ posixSecondsToUTCTime $ realToFrac t

    parseRSON _           = fail "Time"

instance Term ZonedTime where
    toTerm = return . toRSON



-- | Comparing two times is done on the local time, regardless of the timezone.
-- This is exactly how the RethinkDB server does it.
eqTime :: ZonedTime -> ZonedTime -> Bool
eqTime = (==) `on` zonedTimeToUTC



------------------------------------------------------------------------------
-- UTCTime

instance IsDatum  UTCTime
instance IsObject UTCTime

instance FromResponse UTCTime where
    parseResponse = responseAtomParser

instance ToRSON UTCTime where
    toRSON = toRSON . utcToZonedTime utc

instance FromRSON UTCTime where
    parseRSON v = zonedTimeToUTC <$> parseRSON v

instance Term UTCTime where
    toTerm = return . toRSON

instance Lift Exp UTCTime where
    type Simplified UTCTime = ZonedTime
    lift = Constant . utcToZonedTime utc



------------------------------------------------------------------------------
-- | Tables are something you can select objects from.
--
-- This type is not exported, and merely serves as a sort of phantom type. On
-- the client tables are converted to a 'Sequence'.

data Table = MkTable

instance IsSequence Table

instance Term Table where
    toTerm = error "toTerm Table: Server-only type"



------------------------------------------------------------------------------
-- | 'SingleSelection' is essentially a 'Maybe Object', where 'Nothing' is
-- represented with 'Null' in the network protocol.

data SingleSelection = SingleSelection
    deriving (Show)

instance Term SingleSelection where
    toTerm = error "toTerm SingleSelection: Server-only type"

instance IsDatum  SingleSelection
instance IsObject SingleSelection



------------------------------------------------------------------------------
-- | A 'Database' is something which contains tables. It is a server-only
-- type.

data Database = MkDatabase

instance Term Database where
    toTerm = error "toTerm Database: Server-only type"



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
-- | Used in 'OrderBy'.

data Order = Ascending !Text | Descending !Text

instance Term Order where
    toTerm (Ascending  key) = simpleTerm 73 [SomeExp $ Constant $ String key]
    toTerm (Descending key) = simpleTerm 74 [SomeExp $ Constant $ String key]



------------------------------------------------------------------------------
-- | Sequences are a bounded list of items. The server may split the sequence
-- into multiple chunks when sending it to the client. When the response is
-- a partial sequence, the client may request additional chunks until it gets
-- a 'Done'.

data Sequence a
    = Done    !(Vector a)
    | Partial !Token !(Vector a)


class (Term a) => IsSequence a


instance Show (Sequence a) where
    show (Done      v) = "Done " ++ (show $ V.length v)
    show (Partial _ v) = "Partial " ++ (show $ V.length v)

instance (FromRSON a) => FromResponse (Sequence a) where
    parseResponse = responseSequenceParser

instance Term (Sequence a) where
    toTerm = error "toTerm Sequence: Server-only type"

instance IsSequence (Sequence a)



------------------------------------------------------------------------------

data Exp a where
    Constant :: (ToRSON a) => a -> Exp a
    -- Any object which can be converted to RSON can be treated as a constant.
    -- Furthermore, many basic Haskell types have a 'Lift' instance which turns
    -- their values into constants.


    --------------------------------------------------------------------------
    -- Database administration

    ListDatabases  :: Exp (Array Text)
    CreateDatabase :: Exp Text -> Exp Object
    DropDatabase   :: Exp Text -> Exp Object


    --------------------------------------------------------------------------
    -- Table administration

    ListTables     :: Exp Database -> Exp (Array Text)
    CreateTable    :: Exp Database -> Exp Text -> Exp Object
    DropTable      :: Exp Database -> Exp Text -> Exp Object


    --------------------------------------------------------------------------
    -- Index administration

    ListIndices    :: Exp Table -> Exp (Array Text)

    CreateIndex    :: Exp Table -> Exp Text -> (Exp Object -> Exp Datum) -> Exp Object
    -- Create a new secondary index on the table. The index has a name and a
    -- projection function which is applied to every object which is added to the table.

    DropIndex      :: Exp Table -> Exp Text -> Exp Object
    IndexStatus    :: Exp Table -> [Exp Text] -> Exp (Array Object)
    WaitIndex      :: Exp Table -> [Exp Text] -> Exp Object


    Database       :: Exp Text -> Exp Database
    Table          :: Exp Text -> Exp Table

    Coerce         :: (Term a, Term b) => Exp a -> Exp Text -> Exp b
    Eq             :: (IsDatum a, IsDatum b) => Exp a -> Exp b -> Exp Bool
    Ne             :: (IsDatum a, IsDatum b) => Exp a -> Exp b -> Exp Bool
    Match          :: Exp Text -> Exp Text -> Exp Datum
    Get            :: Exp Table -> Exp Text -> Exp SingleSelection
    GetAll         :: (IsDatum a) => Exp Table -> [Exp a] -> Exp (Array Datum)
    GetAllIndexed  :: (IsDatum a) => Exp Table -> [Exp a] -> Text -> Exp (Sequence Datum)

    Add            :: (Num a) => [Exp a] -> Exp a
    Multiply       :: (Num a) => [Exp a] -> Exp a

    All :: [Exp Bool] -> Exp Bool
    -- True if all the elements in the input are True.

    Any :: [Exp Bool] -> Exp Bool
    -- True if any element in the input is True.

    ObjectField :: (IsObject a, IsDatum r) => Exp a -> Exp Text -> Exp r
    -- Get a particular field from an object (or SingleSelection).

    ExtractField :: (IsSequence a) => Exp a -> Exp Text -> Exp a
    -- Like 'ObjectField' but over a sequence.

    Take           :: (IsSequence s) => Exp Double -> Exp s -> Exp s
    Append         :: (IsDatum a) => Exp (Array a) -> Exp a -> Exp (Array a)
    Prepend        :: (IsDatum a) => Exp (Array a) -> Exp a -> Exp (Array a)
    IsEmpty        :: (IsSequence a) => Exp a -> Exp Bool
    Delete         :: (Term a) => Exp a -> Exp Object

    InsertObject   :: Exp Table -> Object -> Exp Object
    -- Insert a single object into the table.

    InsertSequence :: (IsSequence s) => Exp Table -> Exp s -> Exp Object
    -- Insert a sequence into the table.

    Filter :: (IsSequence s, Term a) => (Exp a -> Exp Bool) -> Exp s -> Exp s
    Map :: (IsSequence s, Term a, Term b) => (Exp a -> Exp b) -> Exp s -> Exp s

    Between :: (IsSequence s) => Exp s -> (Bound, Bound) -> Exp s
    -- Select all elements whose primary key is between the two bounds.

    BetweenIndexed :: (IsSequence s) => Exp s -> (Bound, Bound) -> Text -> Exp s
    -- Select all elements whose secondary index is between the two bounds.

    OrderBy :: (IsSequence s) => [Order] -> Exp s -> Exp s
    -- Order a sequence based on the given order specificiation.

    Keys :: (IsObject a) => Exp a -> Exp (Array Text)

    Var :: Int -> Exp a
    -- A 'Var' is used as a placeholder in input to functions.

    Function :: (Term a) => State Context ([Int], Exp a) -> Exp f
    -- Creates a function. The action should take care of allocating an
    -- appropriate number of variables from the context. Note that you should
    -- not use this constructor directly. There are 'Lift' instances for all
    -- commonly used functions.

    Call :: (Term f) => Exp f -> [SomeExp] -> Exp r
    -- Call the given function. The function should take the same number of
    -- arguments as there are provided.


instance (Term a) => Term (Exp a) where
    toTerm (Constant datum) =
        toTerm datum


    toTerm ListDatabases =
        simpleTerm 59 []

    toTerm (CreateDatabase name) =
        simpleTerm 57 [SomeExp name]

    toTerm (DropDatabase name) =
        simpleTerm 58 [SomeExp name]


    toTerm (ListTables db) =
        simpleTerm 62 [SomeExp db]

    toTerm (CreateTable db name) =
        simpleTerm 60 [SomeExp db, SomeExp name]

    toTerm (DropTable db name) =
        simpleTerm 61 [SomeExp db, SomeExp name]


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

    toTerm (Table name) =
        simpleTerm 15 [SomeExp name]

    toTerm (Filter f s) =
        simpleTerm 39 [SomeExp s, SomeExp (lift f)]

    toTerm (Map f s) =
        simpleTerm 38 [SomeExp s, SomeExp (lift f)]

    toTerm (Between s (l, u)) =
        termWithOptions 36 [SomeExp s, SomeExp $ lift (boundDatum l), SomeExp $ lift (boundDatum u)] $
            HMS.fromList
                [ ("left_bound",  String (boundString l))
                , ("right_bound", String (boundString u))
                ]

    toTerm (BetweenIndexed s (l, u) index) =
        termWithOptions 36 [SomeExp s, SomeExp $ lift (boundDatum l), SomeExp $ lift (boundDatum u)] $
            HMS.fromList
                [ ("left_bound",  String (boundString l))
                , ("right_bound", String (boundString u))
                , ("index",       String index)
                ]

    toTerm (OrderBy spec s) = do
        s'    <- toTerm s
        spec' <- mapM toTerm spec
        simpleTerm2 41 ([s'] ++ spec')

    toTerm (InsertObject table object) =
        termWithOptions 56 [SomeExp table, SomeExp (lift object)] emptyOptions

    toTerm (InsertSequence table s) =
        termWithOptions 56 [SomeExp table, SomeExp s] emptyOptions

    toTerm (Delete selection) =
        simpleTerm 54 [SomeExp selection]

    toTerm (ObjectField object field) =
        simpleTerm 31 [SomeExp object, SomeExp field]

    toTerm (ExtractField object field) =
        simpleTerm 31 [SomeExp object, SomeExp field]

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

    toTerm (Match a b) =
        simpleTerm 97 [SomeExp a, SomeExp b]

    toTerm (Get table key) =
        simpleTerm 16 [SomeExp table, SomeExp key]

    toTerm (GetAll table keys) =
        simpleTerm 78 ([SomeExp table] ++ map SomeExp keys)

    toTerm (GetAllIndexed table keys index) =
        termWithOptions 78 ([SomeExp table] ++ map SomeExp keys)
            (HMS.singleton "index" (String index))

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


simpleTerm :: Int -> [SomeExp] -> State Context A.Value
simpleTerm termType args = do
    args' <- mapM toTerm args
    return $ A.Array $ V.fromList [toJSON termType, toJSON args']

simpleTerm2 :: (Term a) => Int -> [a] -> State Context A.Value
simpleTerm2 termType args = do
    args' <- mapM toTerm args
    return $ A.Array $ V.fromList [toJSON termType, toJSON args']

termWithOptions :: Int -> [SomeExp] -> Object -> State Context A.Value
termWithOptions termType args options = do
    args'    <- mapM toTerm args
    options' <- toTerm options

    return $ A.Array $ V.fromList [toJSON termType, toJSON args', toJSON options']


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

instance Lift Exp Double where
    type Simplified Double = Double
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

instance Lift Exp (Array Datum) where
    type Simplified (Array Datum) = (Array Datum)
    lift = Constant

instance (Term r) => Lift Exp (Exp a -> Exp r) where
    type Simplified (Exp a -> Exp r) = Exp r
    lift f = Function $ do
        v1 <- newVar
        return $ ([v1], f (Var v1))

instance (Term r) => Lift Exp (Exp a -> Exp b -> Exp r) where
    type Simplified (Exp a -> Exp b -> Exp r) = Exp r
    lift f = Function $ do
        v1 <- newVar
        v2 <- newVar
        return $ ([v1, v2], f (Var v1) (Var v2))



------------------------------------------------------------------------------
-- 'call1', 'call2' etc generate a function call expression. These should be
-- used instead of the 'Call' constructor because they provide type safety.

-- | Call an unary function with the given argument.
call1 :: (Term a, Term r)
      => (Exp a -> Exp r)
      -> Exp a
      -> Exp r
call1 f a = Call (lift f) [SomeExp a]


-- | Call an binary function with the given arguments.
call2 :: (Term a, Term b, Term r)
      => (Exp a -> Exp b -> Exp r)
      -> Exp a -> Exp b
      -> Exp r
call2 f a b = Call (lift f) [SomeExp a, SomeExp b]


emptyOptions :: Object
emptyOptions = HMS.empty



------------------------------------------------------------------------------
-- | Because the arguments to functions are polymorphic (the individual
-- arguments can, and often have, different types).

data SomeExp where
     SomeExp :: (Term a) => Exp a -> SomeExp

instance Term SomeExp where
    toTerm (SomeExp e) = toTerm e



------------------------------------------------------------------------------
-- | The type of result you get when executing a query of 'Exp a'.
type family Result a

type instance Result Text            = Text
type instance Result Double          = Double
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


responseAtomParser :: (FromRSON a) => Response -> Parser a
responseAtomParser r = case (responseType r, V.toList (responseResult r)) of
    (SuccessAtom, [a]) -> parseRSON a
    _                  -> fail $ "responseAtomParser: Not a single-element vector " ++ show (responseResult r)

responseSequenceParser :: (FromRSON a) => Response -> Parser (Sequence a)
responseSequenceParser r = case responseType r of
    SuccessSequence -> Done    <$> values
    SuccessPartial  -> Partial <$> pure (responseToken r) <*> values
    _               -> fail "responseSequenceParser: Unexpected type"
  where
    values = V.mapM parseRSON (responseResult r)



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
    Response <$> pure token <*> o .: "t" <*> o .: "r"
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
