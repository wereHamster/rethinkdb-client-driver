
module Database.RethinkDB.Messages where


import           Control.Applicative

import           Data.Text            (Text)
import qualified Data.Text            as T
import qualified Data.Text.Encoding   as T

import           Data.Aeson           as A hiding (Result, Object)
import           Data.Aeson.Types     as A hiding (Result, Object)

import           Data.ByteString.Lazy (toStrict)
import qualified Data.ByteString.Lazy as BS

import           Data.Binary
import           Data.Binary.Put
import           Data.Binary.Get as Get

import           Network.Socket (Socket, AddrInfo(..), AddrInfoFlag(..), SocketType(..))
import           Network.Socket (getAddrInfo, socket, connect, close, defaultHints)

import           Network.Socket.ByteString      (recv)
import           Network.Socket.ByteString.Lazy (sendAll)

import           Database.RethinkDB.Types



createSocket :: Text -> Int -> IO Socket
createSocket host port = do
    ai:_ <- getAddrInfo (Just hints) (Just $ T.unpack host) (Just $ show port)
    sock <- socket (addrFamily ai) (addrSocketType ai) (addrProtocol ai)
    connect sock (addrAddress ai)
    return sock
  where
    hints = defaultHints { addrSocketType = Stream, addrFlags = [ AI_NUMERICSERV ] }


closeSocket :: Socket -> IO ()
closeSocket = close


sendMessage :: Socket -> BS.ByteString -> IO ()
sendMessage sock buf = sendAll sock buf


-- | Receive the next message from the socket. If it fails, a (hopefully)
-- descriptive error will be returned.
recvMessage :: Socket -> Get a -> IO (Either Error a)
recvMessage sock parser = go (runGetIncremental parser)
  where
    go (Get.Done _ _ r) = return $ Right r
    go (Get.Partial  c) = recv sock (4 * 1024) >>= go . c . Just
    go (Get.Fail _ _ e) = return $ Left $ ProtocolError $ T.pack e



handshakeMessage :: Maybe Text -> BS.ByteString
handshakeMessage mbAuth = runPut $ do
    -- Protocol version: V0_3
    putWord32le 0x5f75e83e

    -- Authentication
    flip (maybe (putWord32le 0)) mbAuth $ \auth -> do
        putWord32le   $ fromIntegral $ T.length auth
        putByteString $ T.encodeUtf8 auth

    -- Protocol type: JSON
    putWord32le 0x7e6970c7


handshakeReplyParser :: Get Text
handshakeReplyParser = do
    (T.decodeUtf8 . toStrict) <$> getLazyByteStringNul


queryMessage :: Token -> A.Value -> BS.ByteString
queryMessage token msg = runPut $ do
    putWord64host     token
    putWord32le       (fromIntegral $ BS.length buf)
    putLazyByteString buf
  where
    buf = A.encode msg


responseMessageParser :: Get Response
responseMessageParser = do
    token <- getWord64host
    len   <- getWord32le
    buf   <- getByteString (fromIntegral len)

    case A.eitherDecodeStrict buf of
        Left e -> fail $ "responseMessageParser: response is not a JSON value (" ++ e ++ ")"
        Right value -> do
            case A.parseEither (responseParser token) value of
                Left e  -> fail $ "responseMessageParser: could not parse response (" ++ e ++ ")"
                Right x -> return x
