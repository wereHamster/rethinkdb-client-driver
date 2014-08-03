{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# LANGUAGE FlexibleContexts   #-}
{-# LANGUAGE ScopedTypeVariables   #-}

module Main where

import           Control.Applicative

import           Test.Hspec
import           Test.SmallCheck
import           Test.SmallCheck.Series
import           Test.Hspec.SmallCheck

import           Database.RethinkDB

import           Data.Function
import           Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HMS
import           Data.Text           (Text)
import qualified Data.Text           as T
import           Data.Vector         (Vector)
import qualified Data.Vector         as V
import           Data.Time
import           Data.Time.Clock.POSIX



instance Monad m => Serial m Datum
instance Monad m => Serial m UTCTime where
    series = decDepth $ (posixSecondsToUTCTime . fromIntegral) <$> series
instance Monad m => Serial m ZonedTime where
    series = decDepth $ utcToZonedTime utc <$> series
instance Monad m => Serial m Text where
    series = decDepth $ T.pack <$> series
instance Monad m => Serial m (HashMap Text Datum) where
    series = decDepth $ HMS.fromList <$> series
instance Monad m => Serial m (Vector Datum) where
    series = decDepth $ V.fromList <$> series




main :: IO ()
main = do
    h <- newHandle
    hspec $ spec h


spec :: Handle -> Spec
spec h = do

    -- The roundtrips test whether the driver generates the proper terms
    -- and the server responds with what the driver expects.
    describe "roundtrips" $ do
        describe "primitive values" $ do
            it "Double" $ property $ \(x :: Double) ->
                monadic $ ((Right x)==) <$> run h (constant x)
            it "Text" $ property $ \(x :: Text) ->
                monadic $ ((Right x)==) <$> run h (constant x)
            it "Array" $ property $ \(x :: Array) ->
                monadic $ ((Right x)==) <$> run h (constant x)
            it "Object" $ property $ \(x :: Object) ->
                monadic $ ((Right x)==) <$> run h (constant x)
            it "Datum" $ property $ \(x :: Datum) ->
                monadic $ ((Right x)==) <$> run h (constant x)
            it "ZonedTime" $ property $ \(x :: ZonedTime) ->
                monadic $ (on (==) (fmap zonedTimeToUTC) (Right x)) <$> run h (constant x)

        describe "pure functions" $ do
            it "add" $ property $ \(xs0 :: [Double]) -> monadic $ do
                -- The list must not be empty, so we prepend a zero to it.
                let xs = 0 : xs0
                res <- run h $ add $ map constant xs
                return $ res == (Right $ sum xs)
