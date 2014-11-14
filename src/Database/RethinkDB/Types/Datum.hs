{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Database.RethinkDB.Types.Datum where


import           Control.Applicative
import           Control.Monad

import           Data.Text           (Text)
import           Data.Time
import           Data.Scientific
import           System.Locale       (defaultTimeLocale)
import           Data.Time.Clock.POSIX

import           Data.Aeson          (FromJSON(..), ToJSON(..))
import           Data.Aeson.Types    (Value, Parser)
import qualified Data.Aeson          as A

import           Data.Vector         (Vector)
import qualified Data.Vector         as V
import           Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HMS

import           GHC.Generics



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



------------------------------------------------------------------------------
-- | Arrays are vectors of 'Datum'.

type Array a = Vector a



------------------------------------------------------------------------------
-- | Objects are maps from 'Text' to 'Datum'. Like 'Aeson', we're using
-- a strict 'HashMap'.

type Object = HashMap Text Datum



-- | We can't automatically derive 'Eq' because 'ZonedTime' does not have an
-- instance of 'Eq'. See the 'eqTime' function for why we can compare times.
instance Eq Datum where
    (Null    ) == (Null    ) = True
    (Bool   x) == (Bool   y) = x == y
    (Number x) == (Number y) = x == y
    (String x) == (String y) = x == y
    (Array  x) == (Array  y) = x == y
    (Object x) == (Object y) = x == y
    (Time   x) == (Time   y) = (zonedTimeToUTC x) == (zonedTimeToUTC y)
    _          == _          = False


instance ToJSON Datum where
    toJSON (Null    ) = A.Null
    toJSON (Bool   x) = toJSON x
    toJSON (Number x) = toJSON x
    toJSON (String x) = toJSON x
    toJSON (Array  x) = toJSON x
    toJSON (Time   x) = toJSON x
    toJSON (Object x) = toJSON x


instance FromJSON Datum where
    parseJSON   (A.Null    ) = pure Null
    parseJSON   (A.Bool   x) = pure $ Bool x
    parseJSON   (A.Number x) = pure $ Number (realToFrac x)
    parseJSON v@(A.String x) = (Time <$> parseJSON v) <|> (pure $ String x)
    parseJSON   (A.Array  x) = Array <$> V.mapM parseJSON x
    parseJSON   (A.Object x) = do
        -- HashMap does not provide a mapM, what a shame :(
        items <- mapM (\(k, v) -> (,) <$> pure k <*> parseJSON v) $ HMS.toList x
        pure $ Object $ HMS.fromList items



parseWire :: A.Value -> Parser Datum
parseWire (A.Null    ) = pure Null
parseWire (A.Bool   x) = pure $ Bool x
parseWire (A.Number x) = pure $ Number (realToFrac x)
parseWire (A.String x) = pure $ String x
parseWire (A.Array  x) = Array <$> V.mapM parseWire x
parseWire (A.Object x) = (Time <$> zonedTimeParser x) <|> do
    -- HashMap does not provide a mapM, what a shame :(
    items <- mapM (\(k, v) -> (,) <$> pure k <*> parseWire v) $ HMS.toList x
    pure $ Object $ HMS.fromList items


zonedTimeParser :: HashMap Text A.Value -> Parser ZonedTime
zonedTimeParser o = do
    reqlType <- o A..: "$reql_type$"
    guard $ reqlType == ("TIME" :: Text)

    -- Parse the timezone using 'parseTime'. This overapproximates the
    -- possible responses from the server, but better than rolling our
    -- own timezone parser.
    tz <- o A..: "timezone" >>= \tz -> case parseTime defaultTimeLocale "%Z" tz of
        Just d -> pure d
        _      -> fail "Could not parse TimeZone"

    t <- o A..: "epoch_time" :: Parser Double
    pure $ utcToZonedTime tz $ posixSecondsToUTCTime $ realToFrac t




------------------------------------------------------------------------------
-- | Types which can be converted to or from a 'Datum'.

class ToDatum a where
    toDatum :: a -> Datum

class FromDatum a where
    parseDatum :: Datum -> Parser a



(.=) :: ToDatum a => Text -> a -> (Text, Datum)
k .= v = (k, toDatum v)

(.:) :: FromDatum a => HashMap Text Datum -> Text -> Parser a
o .: k = maybe (fail $ "key " ++ show k ++ "not found") parseDatum $ HMS.lookup k o

(.:?) :: FromDatum a => HashMap Text Datum -> Text -> Parser (Maybe a)
o .:? k = maybe (pure Nothing) (fmap Just . parseDatum) $ HMS.lookup k o

object :: [(Text, Datum)] -> Datum
object = Object . HMS.fromList



------------------------------------------------------------------------------
-- Datum

instance ToDatum Datum where
    toDatum = id

instance FromDatum Datum where
    parseDatum = pure



------------------------------------------------------------------------------
-- ()

instance ToDatum () where
    toDatum () = Array V.empty

instance FromDatum () where
    parseDatum (Array x) = if V.null x then pure () else fail "()"
    parseDatum _         = fail "()"



------------------------------------------------------------------------------
-- Bool

instance ToDatum Bool where
    toDatum = Bool

instance FromDatum Bool where
    parseDatum (Bool x) = pure x
    parseDatum _        = fail "Bool"



------------------------------------------------------------------------------
-- Double

instance ToDatum Double where
    toDatum = Number

instance FromDatum Double where
    parseDatum (Number x) = pure x
    parseDatum _          = fail "Double"



------------------------------------------------------------------------------
-- Float

instance ToDatum Float where
    toDatum = Number . realToFrac

instance FromDatum Float where
    parseDatum (Number x) = pure $ realToFrac x
    parseDatum _          = fail "Float"



------------------------------------------------------------------------------
-- Int

instance ToDatum Int where
    toDatum = Number . fromIntegral

instance FromDatum Int where
    parseDatum (Number x) = pure $ floor x
    parseDatum _          = fail "Int"



------------------------------------------------------------------------------
-- Text

instance ToDatum Text where
    toDatum = String

instance FromDatum Text where
    parseDatum (String x) = pure x
    parseDatum _          = fail "Text"



------------------------------------------------------------------------------
-- Array (Vector)

instance (ToDatum a) => ToDatum (Array a) where
    toDatum = Array . V.map toDatum

instance (FromDatum a) => FromDatum (Array a) where
    parseDatum (Array v) = V.mapM parseDatum v
    parseDatum _         = fail "Array"



------------------------------------------------------------------------------
-- Object (HashMap Text Datum)

instance ToDatum Object where
    toDatum = Object

instance FromDatum Object where
    parseDatum (Object o) = do
        -- HashMap does not provide a mapM, what a shame :(
        items <- mapM (\(k, v) -> (,) <$> pure k <*> parseDatum v) $ HMS.toList o
        pure $ HMS.fromList items

    parseDatum _          = fail "Object"



------------------------------------------------------------------------------
-- ZonedTime

instance ToDatum ZonedTime where
    toDatum = Time

instance FromDatum ZonedTime where
    parseDatum (Time x) = pure x
    parseDatum _        = fail "ZonedTime"



------------------------------------------------------------------------------
-- UTCTime

instance ToDatum UTCTime where
    toDatum = Time . utcToZonedTime utc

instance FromDatum UTCTime where
    parseDatum (Time x) = pure (zonedTimeToUTC x)
    parseDatum _        = fail "UTCTime"



------------------------------------------------------------------------------
-- [a]

instance ToDatum a => ToDatum [a] where
    toDatum = Array . V.fromList . map toDatum

instance FromDatum a => FromDatum [a] where
    parseDatum (Array x) = V.toList <$> V.mapM parseDatum x
    parseDatum _         = fail "[a]"



------------------------------------------------------------------------------
-- Maybe a

instance ToDatum a => ToDatum (Maybe a) where
    toDatum Nothing  = Null
    toDatum (Just x) = toDatum x

instance FromDatum a => FromDatum (Maybe a) where
    parseDatum Null = pure Nothing
    parseDatum d    = Just <$> parseDatum d



------------------------------------------------------------------------------
-- Value

instance ToDatum Value where
    toDatum (A.Null    ) = Null
    toDatum (A.Bool   x) = Bool x
    toDatum (A.Number x) = Number $ toRealFloat x
    toDatum (A.String x) = String x
    toDatum (A.Array  x) = Array $ V.map toDatum x
    toDatum (A.Object x) = Object $ fmap toDatum x

instance FromDatum Value where
    parseDatum (Null    ) = pure A.Null
    parseDatum (Bool   x) = pure $ A.Bool x
    parseDatum (Number x) = pure $ A.Number (realToFrac x)
    parseDatum (String x) = pure $ A.String x
    parseDatum (Array  x) = A.Array <$> V.mapM parseDatum x
    parseDatum (Object x) = do
        -- HashMap does not provide a mapM, what a shame :(
        items <- mapM (\(k, v) -> (,) <$> pure k <*> parseDatum v) $ HMS.toList x
        pure $ A.Object $ HMS.fromList items
    parseDatum (Time   x) = pure $ toJSON x
