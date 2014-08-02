
module Database.RethinkDB.Messages where


import           Control.Applicative

import           Data.Text            (Text)
import qualified Data.Text            as T
import qualified Data.Text.Encoding   as T

import           Data.Aeson           as A hiding (Result)
import           Data.Aeson.Types     as A hiding (Result)

import           Data.ByteString.Lazy (toStrict)
import qualified Data.ByteString.Lazy as BS

import           Data.Binary
import           Data.Binary.Put
import           Data.Binary.Get as Get

import           Network.Socket (Socket, AddrInfo(..), AddrInfoFlag(..), SocketType(..))
import           Network.Socket (getAddrInfo, socket, connect, defaultHints)

import           Network.Socket.ByteString      (recv)
import           Network.Socket.ByteString.Lazy (sendAll)

import           Database.RethinkDB.Types



createSocket :: IO Socket
createSocket = do
    ai:_ <- getAddrInfo (Just hints) (Just "localhost") (Just "28015")
    sock <- socket (addrFamily ai) (addrSocketType ai) (addrProtocol ai)
    connect sock (addrAddress ai)
    return sock
  where
    hints = defaultHints { addrSocketType = Stream, addrFlags = [ AI_NUMERICSERV ] }



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



handshakeMessage :: BS.ByteString
handshakeMessage = runPut $ do
    putWord32le 0x5f75e83e -- V0_3
    putWord32le 0          -- No authentication
    putWord32le 0x7e6970c7 -- JSON


handshakeReplyParser :: Get Text
handshakeReplyParser = do
    (T.decodeUtf8 . toStrict) <$> getLazyByteStringNul


queryMessage :: (ToJSON a) => Token -> a -> BS.ByteString
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
    buf   <- getLazyByteString (fromIntegral len)

    let (Just v) = A.decode buf :: (Maybe Value)
    --trace (show v) $ return ()
    case A.parseEither (responseParser token) v of
        Left e -> do
            --trace ("Response parser " ++ e) $ return ()
            fail $ "Response: " ++ e
        Right x  -> return x

