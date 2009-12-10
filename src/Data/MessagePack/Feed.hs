module Data.MessagePack.Feed(
  Feeder,
  feederFromHandle,
  feederFromFile,
  feederFromString,
  ) where

import Control.Monad
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.IORef
import System.IO

type Feeder = IO (Maybe ByteString)

feederFromHandle :: Handle -> IO Feeder
feederFromHandle h = return $ do
  bs <- BS.hGet h bufSize
  if BS.length bs > 0
    then return $ Just bs
    else do
    hClose h
    return Nothing
  where
    bufSize = 4096

feederFromFile :: FilePath -> IO Feeder
feederFromFile path =
  openFile path ReadMode >>= feederFromHandle

feederFromString :: ByteString -> IO Feeder
feederFromString bs = do
  r <- newIORef (Just bs)
  return $ f r
  where
    f r = do
      mb <- readIORef r
      writeIORef r Nothing
      return mb
