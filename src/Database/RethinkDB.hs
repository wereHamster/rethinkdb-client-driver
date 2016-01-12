{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.RethinkDB
    ( Handle
    , defaultPort, newHandle, handleDatabase, close
    , run, nextChunk, collect, stop, wait, serverInfo

    , Error(..)

      -- * The Datum type
    , Datum(..), Array, Object, ToDatum(..), FromDatum(..)
    , (.=), (.:), (.:?), object

      -- The Exp type
    , Exp(..), SomeExp(..)
    , Bound(..), Order(..)
    , Sequence
    , Table, Database, SingleSelection
    , Res, Result, FromResponse
    , ConflictResolutionStrategy(..)
    , emptyOptions
    , lift
    , call1, call2

    , IsDatum, IsObject, IsSequence
    ) where


import           Data.Monoid      ((<>))

import           Data.Text        (Text)
import qualified Data.Text        as T

import qualified Data.Vector      as V
import qualified Data.Aeson.Types as A

import           Network.Socket   (Socket)
import           Data.IORef

import           Database.RethinkDB.Types
import           Database.RethinkDB.Types.Datum
import           Database.RethinkDB.Messages


------------------------------------------------------------------------------
-- Handle

data Handle = Handle
    { hSocket   :: !Socket
    , hTokenRef :: !(IORef Token)

    , hDatabase :: !(Exp Database)
      -- ^ The database which should be used when the 'Table' expression
      -- doesn't specify one.
    }


-- | The default port where RethinkDB accepts client driver connections.
defaultPort :: Int
defaultPort = 28015


-- | Create a new handle to the RethinkDB server.
newHandle :: Text -> Int -> Maybe Text -> Exp Database -> IO Handle
newHandle host port mbAuth db = do
    sock <- createSocket host port

    -- Do the handshake dance. Note that we currently ignore the reply and
    -- assume it is "SUCCESS".
    sendMessage sock (handshakeMessage mbAuth)
    _reply <- recvMessage sock handshakeReplyParser

    -- RethinkDB seems to expect the token to never be null. So we start with
    -- one and then count up.
    ref <- newIORef 1

    return $ Handle sock ref db


-- | The 'Database' which some expressions will use when not explicitly given
-- one (eg. 'Table').
handleDatabase :: Handle -> Exp Database
handleDatabase = hDatabase


-- | Close the given handle. You MUST NOT use the handle after this.
close :: Handle -> IO ()
close handle = closeSocket (hSocket handle)


-- | Start a new query and wait for its (first) result. If the result is an
-- single value ('Datum'), then three will be no further results. If it is
-- a sequence, then you must consume results until the sequence ends.
run :: (FromResponse (Result a))
    => Handle -> Exp a -> IO (Res a)
run handle expr = do
    _token <- start handle expr
    reply <- getResponse handle

    case reply of
        Left e -> return $ Left e
        Right response -> case responseType response of
            ClientErrorType  -> mkError response ClientError
            CompileErrorType -> mkError response CompileError
            RuntimeErrorType -> mkError response RuntimeError
            _                -> return $ parseMessage parseResponse response Right

parseMessage :: (a -> A.Parser b) -> a -> (b -> Either Error c) -> Either Error c
parseMessage parser value f = case A.parseEither parser value of
    Left  e -> Left $ ProtocolError $ T.pack e
    Right v -> f v

mkError :: Response -> (T.Text -> Error) -> IO (Either Error a)
mkError r e = return $ case V.toList (responseResult r) of
    [a] -> parseMessage A.parseJSON a (Left . e)
    _   -> Left $ ProtocolError $ "mkError: Could not parse error" <> T.pack (show (responseResult r))



-- | Collect all the values in a sequence and make them available as
-- a 'Vector a'.
collect :: (FromDatum a) => Handle -> Sequence a -> IO (Either Error (V.Vector a))
collect _        (Done      x) = return $ Right x
collect handle s@(Partial _ x) = do
    chunk <- nextChunk handle s
    case chunk of
        Left e -> return $ Left e
        Right r -> do
            vals <- collect handle r
            case vals of
                Left ve -> return $ Left ve
                Right v -> return $ Right $ x <> v



-- | Start a new query. Returns the 'Token' which can be used to track its
-- progress.
start :: Handle -> Exp a -> IO Token
start handle term = do
    token <- atomicModifyIORef (hTokenRef handle) (\x -> (x + 1, x))
    sendMessage (hSocket handle) (queryMessage token msg)
    return token

  where
    msg = compileTerm (hDatabase handle) $ do
        term'    <- toTerm term
        options' <- toTerm emptyOptions
        return $ A.Array $ V.fromList
            [ A.Number 1
            , term'
            , A.toJSON $ options'
            ]


singleElementArray :: Int -> A.Value
singleElementArray x = A.Array $ V.singleton $ A.Number $ fromIntegral x

-- | Let the server know that it can send the next response corresponding to
-- the given token.
continue :: Handle -> Token -> IO ()
continue handle token = sendMessage
    (hSocket handle)
    (queryMessage token $ singleElementArray 2)


-- | Stop (abort?) a query.
stop :: Handle -> Token -> IO ()
stop handle token = sendMessage
    (hSocket handle)
    (queryMessage token $ singleElementArray 3)


-- | Wait until a previous query (which was started with the 'noreply' option)
-- finishes.
wait :: Handle -> Token -> IO ()
wait handle token = sendMessage
    (hSocket handle)
    (queryMessage token $ singleElementArray 4)



serverInfo :: Handle -> IO (Either Error ServerInfo)
serverInfo handle = do
    token <- atomicModifyIORef (hTokenRef handle) (\x -> (x + 1, x))
    sendMessage (hSocket handle) (queryMessage token $ singleElementArray 5)
    reply <- getResponse handle
    case reply of
        Left e -> return $ Left e
        Right res -> return $ parseMessage parseResponse res Right


-- | Get the next chunk of a sequence. It is an error to request the next chunk
-- if the sequence is already 'Done',
nextChunk :: (FromResponse (Sequence a))
          => Handle -> Sequence a -> IO (Either Error (Sequence a))
nextChunk _      (Done          _) = return $ Left $ ProtocolError ""
nextChunk handle (Partial token _) = do
    continue handle token
    reply <- getResponse handle
    case reply of
        Left e -> return $ Left e
        Right response -> return $ parseMessage parseResponse response Right


getResponse :: Handle -> IO (Either Error Response)
getResponse handle = do
    recvMessage (hSocket handle) responseMessageParser
