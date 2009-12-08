module Data.MessagePack.Stream(
  unpackObjects,
  unpackObjectsFromFile,
  unpackObjectsFromHandle,
  unpackObjectsFromString,
  ) where

import Control.Monad
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import System.IO
import System.IO.Unsafe

import Data.MessagePack.Base
import Data.MessagePack.Class

unpackObjects :: Unpacker -> (IO (Maybe ByteString)) -> IO [Object]
unpackObjects up feeder = f where
  f = unsafeInterleaveIO $ do
    mbo <- unpackOnce
    case mbo of
      Just o -> do
        os <- f
        return $ o:os
      Nothing ->
        return []

  unpackOnce = do
    resp <- unpackerExecute up
    case resp of
      0 -> do
        r <- feedOnce
        if r
          then unpackOnce
          else return Nothing
      1 -> do
        liftM Just $ unpackerData up
      _ ->
        error $ "unpackerExecute fails: " ++ show resp

  feedOnce = do
    dat <- feeder
    case dat of
      Nothing ->
        return False
      Just bs -> do
        unpackerFeed up bs
        return True

unpackObjectsFromFile :: FilePath -> IO [Object]
unpackObjectsFromFile fname = do
  h <- openFile fname ReadMode
  unpackObjectsFromHandle h

unpackObjectsFromHandle :: Handle -> IO [Object]
unpackObjectsFromHandle h = do
  up <- newUnpacker defaultInitialBufferSize
  unpackObjects up f
  where
    bufSize = 4096
    f = do
      bs <- BS.hGet h bufSize
      if BS.length bs > 0
        then return $ Just bs
        else return Nothing

unpackObjectsFromString :: ByteString -> IO [Object]
unpackObjectsFromString bs = do
  up <- newUnpacker defaultInitialBufferSize
  unpackerFeed up bs
  unpackObjects up $ return Nothing
