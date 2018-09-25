{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}

module Database.RethinkDB
    ( Handle
    , defaultPort, newHandle, handleDatabase, close, serverInfo

      -- * High-level query API
    , run, nextChunk, collect

      -- * Low-level query API
    , start, continue, stop, wait, nextResult

    , Token, Error(..), Response(..), ChangeNotification(..)

      -- * The Datum type
    , Datum(..), Array, Object, ToDatum(..), FromDatum(..)
    , (.=), (.:), (.:?), object

      -- The Exp type
    , Exp(..), SomeExp(..)
    , Bound(..), Order(..)
    , Sequence(..)
    , Table, Database, SingleSelection
    , Res, Result, FromResponse
    , ConflictResolutionStrategy(..)
    , emptyOptions
    , lift
    , call1, call2

    , IsDatum, IsObject, IsSequence
    ) where


import           Control.Monad
import           Control.Concurrent
import           Control.Concurrent.STM

import           Data.Monoid      ((<>))

import           Data.Text        (Text)
import qualified Data.Text        as T

import           Data.Vector      (Vector)
import qualified Data.Vector      as V

import           Data.Map.Strict  (Map)
import qualified Data.Map.Strict  as M

import qualified Data.Aeson.Types as A

import           Data.Sequence    (Seq, ViewR(..))
import qualified Data.Sequence    as S

import           Data.IORef

import           Network.Socket   (Socket)

import           Database.RethinkDB.Types
import           Database.RethinkDB.Types.Datum
import           Database.RethinkDB.Messages


------------------------------------------------------------------------------
-- Handle

data Handle = Handle
    { hSocket :: !(MVar Socket)
      -- ^ Any thread can write to the socket. In theory. But I don't think
      -- Haskell allows atomic writes to a socket, so it is protected inside
      -- an 'MVar'.
      --
      -- When too many threads write to the socket, this may cause resource
      -- contention. Users are encouraged to use a resource pool to alleviate
      -- that.

    , hTokenRef :: !(IORef Token)
      -- ^ This is used to allocate new tokens. We use 'atomicModifyIORef' to
      -- efficiently allocate new tokens.
      --
      -- RethinkDB seems to expect the token to never be zero. So we need to
      -- start with one and then count up.

    , hError :: !(TVar (Maybe Error))
      -- ^ If there was a fatal error while reading from the socket, it will
      -- be stored here. If this is set then no further replies will be
      -- processed. The user needs to close and re-open the handle to recover.

    , hResponses :: !(TVar (Map Token (Seq (Either Error Response))))
      -- ^ Responses to queries. A thread reads the responses from the socket
      -- and pushes them into the queues.

    , hReader :: !ThreadId
      -- ^ Thread which reads from the socket and copies responses into the
      -- queues in 'hResponses'.

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

    err       <- newTVarIO Nothing
    responses <- newTVarIO M.empty

    readerThreadId <- forkIO $ forever $ do
        res <- recvMessage sock responseMessageParser
        case res of
            Left e -> atomically $ do
                mbError <- readTVar err
                case mbError of
                    Nothing -> writeTVar err (Just e)
                    Just _  -> pure ()

            Right (Left (token, msg)) -> atomically $ modifyTVar' responses $
                M.insertWith mappend token (S.singleton $ Left $ ProtocolError $ T.pack msg)

            Right (Right r) -> atomically $ modifyTVar' responses $
                M.insertWith mappend (responseToken r) (S.singleton $ Right r)

        return ()

    Handle
        <$> newMVar sock
        <*> newIORef 1
        <*> pure err
        <*> pure responses
        <*> pure readerThreadId
        <*> pure db


-- | The 'Database' which some expressions will use when not explicitly given
-- one (eg. 'Table').
handleDatabase :: Handle -> Exp Database
handleDatabase = hDatabase


-- | Close the given handle. You MUST NOT use the handle after this.
close :: Handle -> IO ()
close handle = do
    withMVar (hSocket handle) closeSocket
    killThread (hReader handle)


serverInfo :: Handle -> IO (Either Error ServerInfo)
serverInfo handle = do
    token <- atomicModifyIORef' (hTokenRef handle) (\x -> (x + 1, x))
    withMVar (hSocket handle) $ \socket ->
        sendMessage socket (queryMessage token $ singleElementArray 5)
    nextResult handle token



--------------------------------------------------------------------------------
-- * High-level query API
--
-- These are synchronous functions, they make it really easy to run a query and
-- immediately get its results.
--
-- If the result is a sequence, you can either manually iterate through the
-- chunks ('nextChunk') or fetch the whole sequence at once ('collect').


-- | Start a new query and wait for its (first) result. If the result is an
-- single value ('Datum'), then there will be no further results. If it is
-- a sequence, then you must consume results until the sequence ends.
run :: (FromResponse (Result a)) => Handle -> Exp a -> IO (Res a)
run handle expr = do
    token <- start handle expr
    nextResult handle token


-- | Get the next chunk of a sequence. It is an error to request the next chunk
-- if the sequence is already 'Done',
nextChunk :: (FromResponse (Sequence a))
          => Handle -> Sequence a -> IO (Either Error (Sequence a))
nextChunk _      (Done          _) = return $ Left $ ProtocolError "nextChunk: Done"
nextChunk handle (Partial token _) = do
    continue handle token
    nextResult handle token


-- | Collect all the values in a sequence and make them available as
-- a 'Vector a'.
collect :: (FromDatum a) => Handle -> Sequence a -> IO (Either Error (Vector a))
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



--------------------------------------------------------------------------------
-- * Low-level query API
--
-- These functions map almost verbatim to the wire protocol messages. They are
-- asynchronous, you can send multiple queries and get the corresponding
-- responses sometime later.


-- | Start a new query. Returns the 'Token' which can be used to track its
-- progress.
start :: Handle -> Exp a -> IO Token
start handle term = do
    token <- atomicModifyIORef' (hTokenRef handle) (\x -> (x + 1, x))
    withMVar (hSocket handle) $ \socket ->
        sendMessage socket (queryMessage token msg)
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
continue handle token = withMVar (hSocket handle) $ \socket ->
    sendMessage socket (queryMessage token $ singleElementArray 2)


-- | Stop (abort?) a query.
stop :: Handle -> Token -> IO ()
stop handle token = withMVar (hSocket handle) $ \socket ->
    sendMessage socket (queryMessage token $ singleElementArray 3)


-- | Wait until a previous query (which was started with the 'noreply' option)
-- finishes.
wait :: Handle -> Token -> IO ()
wait handle token = withMVar (hSocket handle) $ \socket ->
    sendMessage socket (queryMessage token $ singleElementArray 4)



-- | This function blocks until there is a response ready for the query with
-- the given token. It may block indefinitely if the token refers to a query
-- which has already finished or does not exist yet!
responseForToken :: Handle -> Token -> IO (Either Error Response)
responseForToken h token = atomically $ do
    m <- readTVar (hResponses h)
    case M.lookup token m of
        Nothing -> retry
        Just s -> case S.viewr s of
            EmptyR -> retry
            rest :> a -> do
                modifyTVar' (hResponses h) $ if S.null rest
                    then M.delete token
                    else M.insert token rest

                pure a


nextResult :: (FromResponse a) => Handle -> Token -> IO (Either Error a)
nextResult h token = do
    mbError <- atomically $ readTVar (hError h)
    case mbError of
        Just err -> return $ Left err
        Nothing  -> do
            errorOrResponse <- responseForToken h token
            case errorOrResponse of
                Left err -> return $ Left err
                Right response -> case responseType response of
                    ClientErrorType  -> mkError response ClientError
                    CompileErrorType -> mkError response CompileError
                    RuntimeErrorType -> mkError response RuntimeError
                    _                -> return $ parseMessage parseResponse response Right


parseMessage :: (a -> A.Parser b) -> a -> (b -> Either Error c) -> Either Error c
parseMessage parser value f = case A.parseEither parser value of
    Left  e -> Left $ ProtocolError $ T.pack e
    Right v -> f v

mkError :: Response -> (Text -> Error) -> IO (Either Error a)
mkError r e = return $ case V.toList (responseResult r) of
    [a] -> parseMessage A.parseJSON a (Left . e)
    _   -> Left $ ProtocolError $ "mkError: Could not parse error" <> T.pack (show (responseResult r))
