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
-- | A class describing a type which can be converted to the RethinkDB-specific
-- wire protocol. It is based on JSON, but certain types use a presumably more
-- efficient encoding.

class FromRSON a where
    parseRSON :: A.Value -> Parser a

------------------------------------------------------------------------------
-- | See 'FromRSON'.

class ToRSON a where
    toRSON :: a -> A.Value

instance (ToRSON a, ToRSON b) => ToRSON (a,b) where
    toRSON (a,b) = toJSON (toRSON a, toRSON b)



------------------------------------------------------------------------------
-- | Any value which can appear in RQL terms.
--
-- For convenience we require that it can be converted to JSON, but that is
-- not required for all types. Only types which satisfy 'IsDatum' are
-- eventually converted to JSON.

class (ToRSON a) => Any a



------------------------------------------------------------------------------
-- | A sumtype covering all the primitive types which can appear in queries
-- or responses.

data Datum
    = Null
    | Bool   !Bool
    | Number !Double
    | String !Text
    | Array  !(Array Datum)
    | Object !Object
    | Time   !ZonedTime
    deriving (Show, Generic)


class (Any a) => IsDatum a


instance Any     Datum
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

instance FromResponse Datum where
    parseResponse = responseAtomParser



------------------------------------------------------------------------------
-- | For a boolean type, we're reusing the standard Haskell 'Bool' type.

instance Any     Bool
instance IsDatum Bool

instance FromResponse Bool where
    parseResponse = responseAtomParser

instance FromRSON Bool where
    parseRSON = parseJSON

instance ToRSON Bool where
    toRSON = toJSON



------------------------------------------------------------------------------
-- | Numbers are 'Double' (unlike 'Aeson', which uses 'Scientific'). No
-- particular reason.

instance Any     Double
instance IsDatum Double

instance FromResponse Double where
    parseResponse = responseAtomParser

instance FromRSON Double where
    parseRSON = parseJSON

instance ToRSON Double where
    toRSON = toJSON



------------------------------------------------------------------------------
-- | For strings, we're using the Haskell 'Text' type.

instance Any     Text
instance IsDatum Text

instance FromResponse Text where
    parseResponse = responseAtomParser

instance FromRSON Text where
    parseRSON = parseJSON

instance ToRSON Text where
    toRSON = toJSON



------------------------------------------------------------------------------
-- | Arrays are vectors of 'Datum'.

type Array a = Vector a

instance (Any a)     => Any        (Array a)
instance (IsDatum a) => IsDatum    (Array a)
instance (IsDatum a) => IsSequence (Array a)

instance (FromRSON a) => FromResponse (Array a) where
    parseResponse = responseAtomParser

-- Arrays are encoded as a term MAKE_ARRAY (2).
instance (ToRSON a) => ToRSON (Array a) where
    toRSON v = A.Array $ V.fromList $
        [ A.Number 2
        , toJSON $ map toRSON (V.toList v)
        , toRSON emptyOptions
        ]

instance (FromRSON a) => FromRSON (Array a) where
    parseRSON (A.Array v) = V.mapM parseRSON v
    parseRSON _           = fail "Array"



------------------------------------------------------------------------------
-- | Objects are maps from 'Text' to 'Datum'. Like 'Aeson', we're using
-- 'HashMap'.

type Object = HashMap Text Datum


class (IsDatum a) => IsObject a


instance Any      Object
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
    toRSON = A.Object . HMS.fromList . map (\(k, v) -> (k, toRSON v)) . HMS.toList



------------------------------------------------------------------------------
-- | Time in RethinkDB is represented similar to the 'ZonedTime' type. Except
-- that the JSON representation on the wire looks different from the default
-- used by 'Aeson'. Therefore we have a custom 'FromRSON' and 'ToRSON'
-- instances.

instance Any      ZonedTime
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


-- | Comparing two times is done on the local time, regardless of the timezone.
-- This is exactly how the RethinkDB server does it.
eqTime :: ZonedTime -> ZonedTime -> Bool
eqTime = (==) `on` zonedTimeToUTC



------------------------------------------------------------------------------
-- | Tables are something you can select objects from.
--
-- This type is not exported, and merely serves as a sort of phantom type. On
-- the client tables are converted to a 'Sequence'.

data Table = MkTable

instance Any        Table
instance IsSequence Table

instance ToRSON Table where
    toRSON = error "toRSON Table: Server-only type"



------------------------------------------------------------------------------
-- | 'SingleSelection' is essentially a 'Maybe Object', where 'Nothing' is
-- represented with 'Null' in the network protocol.

data SingleSelection = SingleSelection
    deriving (Show)

instance ToRSON SingleSelection where
    toRSON = error "toRSON SingleSelection: Server-only type"

instance Any      SingleSelection
instance IsDatum  SingleSelection
instance IsObject SingleSelection



------------------------------------------------------------------------------
-- | A 'Database' is something which contains tables. It is a server-only
-- type.

data Database = MkDatabase

instance Any Database
instance ToRSON Database where
    toRSON = error "toRSON Database: Server-only type"



------------------------------------------------------------------------------
-- | Sequences are a bounded list of items. The server may split the sequence
-- into multiple chunks when sending it to the client. When the response is
-- a partial sequence, the client may request additional chunks until it gets
-- a 'Done'.

data Sequence a
    = Done    !(Vector a)
    | Partial !Token !(Vector a)


class Any a => IsSequence a


instance Show (Sequence a) where
    show (Done      v) = "Done " ++ (show $ V.length v)
    show (Partial _ v) = "Partial " ++ (show $ V.length v)

instance (FromRSON a) => FromResponse (Sequence a) where
    parseResponse = responseSequenceParser

instance ToRSON (Sequence a) where
    toRSON = error "toRSON Sequence: Server-only type"

instance (Any a) => Any (Sequence a)
instance (Any a) => IsSequence (Sequence a)



------------------------------------------------------------------------------

data Exp a where
    Constant       :: (IsDatum a) => a -> Exp a

    -- Database administration
    ListDatabases  :: Exp (Array Text)
    CreateDatabase :: Exp Text -> Exp Object
    DropDatabase   :: Exp Text -> Exp Object

    -- Table administration
    ListTables     :: Exp Database -> Exp (Array Text)
    CreateTable    :: Exp Database -> Exp Text -> Exp Object
    DropTable      :: Exp Database -> Exp Text -> Exp Object

    -- Index administration
    ListIndices    :: Exp Table -> Exp (Array Text)
    CreateIndex    :: Exp Table -> Exp Text -> Exp Object
    DropIndex      :: Exp Table -> Exp Text -> Exp Object
    IndexStatus    :: Exp Table -> [Exp Text] -> Exp (Array Object)
    WaitIndex      :: Exp Table -> [Exp Text] -> Exp Object

    Database       :: Exp Text -> Exp Database
    Table          :: Exp Text -> Exp Table
    Coerce         :: (Any a, Any b) => Exp a -> Exp Text -> Exp b
    Add            :: [Exp Double] -> Exp Double
    Eq             :: (Any a, Any b) => Exp a -> Exp b -> Exp Bool
    Get            :: Exp Table -> Exp Text -> Exp SingleSelection
    GetAll         :: (IsDatum a) => Exp Table -> [Exp a] -> Exp (Array Datum)
    GetAllIndexed  :: (IsDatum a) => Exp Table -> [Exp a] -> Text -> Exp (Sequence Datum)

    ObjectField :: (IsObject a) => Exp a -> Exp Text -> Exp Datum
    -- Get a particular field from an object (or SingleSelection).

    ExtractField :: (IsSequence a) => Exp a -> Exp Text -> Exp a
    -- Like 'ObjectField' but over a sequence.

    Take           :: (Any a) => Exp (Sequence a) -> Exp Double -> Exp (Sequence a)
    Append         :: (Any a) => Exp (Array a) -> Exp a -> Exp (Array a)
    Prepend        :: (Any a) => Exp (Array a) -> Exp a -> Exp (Array a)
    IsEmpty        :: (IsSequence a) => Exp a -> Exp Bool
    Delete         :: (Any a) => Exp a -> Exp Object

    InsertObject   :: Exp Table -> Object -> Exp Object
    -- Insert a single object into the table.

    InsertSequence :: (IsSequence s) => Exp Table -> Exp s -> Exp Object
    -- Insert a sequence into the table.

    Filter         :: (IsSequence s, Any f) => Exp s -> Exp f -> Exp s
    Keys           :: (IsObject a) => Exp a -> Exp (Array Text)

instance (ToRSON a) => ToRSON (Exp a) where
    toRSON (Constant datum) =
        toRSON datum


    toRSON ListDatabases =
        simpleTerm 59 []

    toRSON (CreateDatabase name) =
        simpleTerm 57 [SomeExp name]

    toRSON (DropDatabase name) =
        simpleTerm 58 [SomeExp name]


    toRSON (ListTables db) =
        simpleTerm 62 [SomeExp db]

    toRSON (CreateTable db name) =
        simpleTerm 60 [SomeExp db, SomeExp name]

    toRSON (DropTable db name) =
        simpleTerm 61 [SomeExp db, SomeExp name]


    toRSON (ListIndices table) =
        simpleTerm 77 [SomeExp table]

    toRSON (CreateIndex table name) =
        simpleTerm 75 [SomeExp table, SomeExp name]

    toRSON (DropIndex table name) =
        simpleTerm 76 [SomeExp table, SomeExp name]

    toRSON (IndexStatus table indices) =
        simpleTerm 139 ([SomeExp table] ++ map SomeExp indices)

    toRSON (WaitIndex table indices) =
        simpleTerm 140 ([SomeExp table] ++ map SomeExp indices)


    toRSON (Database name) =
        simpleTerm 14 [SomeExp name]

    toRSON (Table name) =
        simpleTerm 15 [SomeExp name]

    toRSON (Filter s f) =
        simpleTerm 39 [SomeExp s, SomeExp f]

    toRSON (InsertObject table object) =
        termWithOptions 56 [SomeExp table, SomeExp (constant object)] emptyOptions

    toRSON (InsertSequence table s) =
        termWithOptions 56 [SomeExp table, SomeExp s] emptyOptions

    toRSON (Delete selection) =
        simpleTerm 54 [SomeExp selection]

    toRSON (ObjectField object field) =
        simpleTerm 31 [SomeExp object, SomeExp field]

    toRSON (ExtractField object field) =
        simpleTerm 31 [SomeExp object, SomeExp field]

    toRSON (Coerce value typeName) =
        simpleTerm 51 [SomeExp value, SomeExp typeName]

    toRSON (Add values) =
        simpleTerm 24 (map SomeExp values)

    toRSON (Eq a b) =
        simpleTerm 17 [SomeExp a, SomeExp b]

    toRSON (Get table key) =
        simpleTerm 16 [SomeExp table, SomeExp key]

    toRSON (GetAll table keys) =
        simpleTerm 78 ([SomeExp table] ++ map SomeExp keys)

    toRSON (GetAllIndexed table keys index) =
        termWithOptions 78 ([SomeExp table] ++ map SomeExp keys)
            (HMS.singleton "index" (String index))

    toRSON (Take s n) =
        simpleTerm 71 [SomeExp s, SomeExp n]

    toRSON (Append array value) =
        simpleTerm 29 [SomeExp array, SomeExp value]

    toRSON (Prepend array value) =
        simpleTerm 80 [SomeExp array, SomeExp value]

    toRSON (IsEmpty s) =
        simpleTerm 86 [SomeExp s]

    toRSON (Keys a) =
        simpleTerm 94 [SomeExp a]


simpleTerm :: Int -> [SomeExp] -> A.Value
simpleTerm termType args =
    A.Array $ V.fromList [toJSON termType, toJSON (map toRSON args)]

termWithOptions :: Int -> [SomeExp] -> Object -> A.Value
termWithOptions termType args options =
    A.Array $ V.fromList [toJSON termType, toJSON (map toRSON args), toRSON options]


-- | Convenience to for automatically converting a 'Text' to a constant
-- expression.
instance IsString (Exp Text) where
   fromString = constant . fromString


-- | Convert a 'Datum' to an 'Exp'.
constant :: (IsDatum a) => a -> Exp a
constant x = Constant x


emptyOptions :: Object
emptyOptions = HMS.empty



------------------------------------------------------------------------------
-- | Because the arguments to functions are polymorphic (the individual
-- arguments can, and often have, different types).

data SomeExp where
     SomeExp :: (ToRSON a, Any a) => Exp a -> SomeExp

instance ToRSON SomeExp where
    toRSON (SomeExp e) = toRSON e



------------------------------------------------------------------------------
-- Query

data Query a
    = Start (Exp a) [(Text, SomeExp)]
    | Continue
    | Stop
    | NoreplyWait

instance (ToRSON a) => ToRSON (Query a) where
    toRSON (Start term options) = A.Array $ V.fromList
        [ A.Number 1
        , toRSON term
        , toJSON $ map toRSON options
        ]
    toRSON Continue     = A.Array $ V.singleton (A.Number 2)
    toRSON Stop         = A.Array $ V.singleton (A.Number 3)
    toRSON NoreplyWait  = A.Array $ V.singleton (A.Number 4)



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
-- Errors

data Error
    = ProtocolError !Text
      -- ^ An error on the protocol level. Perhaps the socket was closed
      -- unexpectedly, or the server sent a message which the driver could
      -- not parse.

    | ClientError
    | CompileError
    | RuntimeError
    deriving (Eq, Show)
