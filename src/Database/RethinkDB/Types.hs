{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Database.RethinkDB.Types where


import           Control.Applicative

import           Data.Word
import           Data.String
import           Data.Text (Text)

import           Data.Aeson          (FromJSON(..), ToJSON(..), (.:))
import           Data.Aeson.Types    (Parser, Value)
import qualified Data.Aeson       as A

import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HMS

import           GHC.Generics



------------------------------------------------------------------------------
-- | Any value which can appear in RQL terms.
--
-- For convenience we require that it can be converted to JSON, but that is
-- not required for all types. Only types which satisfy 'IsDatum' are
-- eventually converted to JSON.

class (ToJSON a) => Any a



------------------------------------------------------------------------------
-- | A sumtype covering all the primitive types which can appear in queries
-- or responses.

data Datum
    = Null
    | Bool   !Bool
    | Number !Double
    | String !Text
    | Array  !Array
    | Object !Object
    deriving (Eq, Show, Generic)


class (Any a) => IsDatum a


instance Any     Datum
instance IsDatum Datum

instance ToJSON Datum where
    toJSON (Null    ) = A.Null
    toJSON (Bool   x) = toJSON x
    toJSON (Number x) = toJSON x
    toJSON (String x) = toJSON x
    toJSON (Array  x) = toJSON x
    toJSON (Object x) = toJSON x

instance FromJSON Datum where
    parseJSON (A.Null    ) = pure Null
    parseJSON (A.Bool   x) = pure $ Bool x
    parseJSON (A.Number x) = pure $ Number (realToFrac x)
    parseJSON (A.String x) = pure $ String x
    parseJSON (A.Array  x) = Array <$> V.mapM parseJSON x
    parseJSON (A.Object x) = do
        -- HashMap does not provide a mapM, what a shame :(
        items <- mapM (\(k, v) -> (,) <$> pure k <*> parseJSON v) $ HMS.toList x
        return $ Object $ HMS.fromList items

instance FromResponse Datum where
    parseResponse = responseAtomParser



------------------------------------------------------------------------------
-- | For a boolean type, we're reusing the standard Haskell 'Bool' type.

instance Any     Bool
instance IsDatum Bool

instance FromResponse Bool where
    parseResponse = responseAtomParser



------------------------------------------------------------------------------
-- | Numbers are 'Double' (unlike 'Aeson', which uses 'Scientific'). No
-- particular reason.

instance Any     Double
instance IsDatum Double

instance FromResponse Double where
    parseResponse = responseAtomParser



------------------------------------------------------------------------------
-- | For strings, we're using the Haskell 'Text' type.

instance Any     Text
instance IsDatum Text

instance FromResponse Text where
    parseResponse = responseAtomParser



------------------------------------------------------------------------------
-- | Arrays are vectors of 'Datum'.

type Array = Vector Datum

instance Any     Array
instance IsDatum Array

instance FromResponse Array where
    parseResponse = responseAtomParser

-- Arrays are encoded as a term MAKE_ARRAY.
instance ToJSON Array where
    toJSON v = A.Array $ V.fromList $
        [ toJSON MAKE_ARRAY
        , toJSON $ map toJSON (V.toList v)
        , toJSON emptyOptions
        ]

instance FromJSON Array where
    parseJSON (A.Array v) = V.mapM parseJSON v
    parseJSON _           = fail "Array"



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



------------------------------------------------------------------------------
-- | Tables are something you can select objects from.
--
-- This type is not exported, and merely serves as a sort of phantom type. On
-- the client tables are converted to a 'Sequence'.

data Table = Table

instance Any        Table
instance IsSequence Table

instance ToJSON Table where
    toJSON = error "toJSON Table: Server-only type"



------------------------------------------------------------------------------
-- | 'SingleSelection' is essentially a 'Maybe Object', where 'Nothing' is
-- represented with 'Null' in the network protocol.

data SingleSelection = SingleSelection
    deriving (Show)

instance ToJSON SingleSelection where
    toJSON = error "toJSON SingleSelection: Server-only type"

instance Any      SingleSelection
instance IsDatum  SingleSelection
instance IsObject SingleSelection



------------------------------------------------------------------------------
-- | A 'Database' is something which contains tables. It is a server-only
-- type.

data Database = Database

instance Any Database
instance ToJSON Database where
    toJSON = error "toJSON Database: Server-only type"



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

instance (FromJSON a) => FromResponse (Sequence a) where
    parseResponse = responseSequenceParser

instance ToJSON (Sequence a) where
    toJSON = error "toJSON Sequence: Server-only type"

instance (Any a) => Any (Sequence a)
instance (Any a) => IsSequence (Sequence a)



------------------------------------------------------------------------------
-- | All types of functions which the server supports. Keep this in sync with
-- the protocol definition file, especially the ToJSON instance.

data TermType
    = ADD
    | COERCE_TO
    | DB
    | GET
    | GET_ALL
    | GET_FIELD
    | INSERT
    | LIMIT
    | MAKE_ARRAY
    | APPEND
    | TABLE


instance ToJSON TermType where
    toJSON MAKE_ARRAY = A.Number 2
    toJSON DB         = A.Number 14
    toJSON TABLE      = A.Number 15
    toJSON GET        = A.Number 16
    toJSON GET_ALL    = A.Number 78
    toJSON ADD        = A.Number 24
    toJSON COERCE_TO  = A.Number 51
    toJSON GET_FIELD  = A.Number 31
    toJSON INSERT     = A.Number 56
    toJSON LIMIT      = A.Number 71
    toJSON APPEND     = A.Number 29



------------------------------------------------------------------------------

data Exp a where
    Constant :: (IsDatum a) => a -> Exp a
    Term     :: TermType -> [SomeExp] -> Object -> Exp a


instance (ToJSON a) => ToJSON (Exp a) where
    toJSON (Constant datum) =
        toJSON datum

    toJSON (Term termType args opts) =
        toJSON [toJSON termType, toJSON args, toJSON opts]


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
     SomeExp :: (ToJSON a, Any a) => Exp a -> SomeExp

instance ToJSON SomeExp where
    toJSON (SomeExp e) = toJSON e



------------------------------------------------------------------------------
-- Query

data Query a
    = Start (Exp a) [(Text, SomeExp)]
    | Continue
    | Stop
    | NoreplyWait

instance (ToJSON a) => ToJSON (Query a) where
    toJSON (Start term options) = A.Array $ V.fromList
        [ A.Number 1
        , toJSON term
        , toJSON options
        ]
    toJSON Continue     = A.Array $ V.singleton (A.Number 2)
    toJSON Stop         = A.Array $ V.singleton (A.Number 3)
    toJSON NoreplyWait  = A.Array $ V.singleton (A.Number 4)



------------------------------------------------------------------------------
-- | The type of result you get when executing a query of 'Exp a'.
type family Result a

type instance Result Text            = Text
type instance Result Double          = Double
type instance Result Bool            = Bool

type instance Result Table           = Sequence Datum
type instance Result Datum           = Datum
type instance Result Object          = Object
type instance Result Array           = Array
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


responseAtomParser :: (FromJSON a) => Response -> Parser a
responseAtomParser r = case (responseType r, V.toList (responseResult r)) of
    (SuccessAtom, [a]) -> parseJSON a
    _                  -> fail $ "responseAtomParser: Not a single-element vector " ++ show (responseResult r)

responseSequenceParser :: (FromJSON a) => Response -> Parser (Sequence a)
responseSequenceParser r = case responseType r of
    SuccessSequence -> Done    <$> values
    SuccessPartial  -> Partial <$> pure (responseToken r) <*> values
    _               -> fail "responseSequenceParser: Unexpected type"
  where
    values = V.mapM parseJSON (responseResult r)



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
