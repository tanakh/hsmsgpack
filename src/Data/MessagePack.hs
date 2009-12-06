module Data.MessagePack(
  module Data.MessagePack.Base,
  module Data.MessagePack.Class,
  
  packb,
  unpackb,
  
  packb',
  unpackb',
  ) where

import Data.ByteString (ByteString)
import System.IO.Unsafe

import Data.MessagePack.Base
import Data.MessagePack.Class

packb :: OBJECT a => a -> IO ByteString
packb dat = do
  sb <- newSimpleBuffer
  pc <- newPacker sb
  pack pc dat
  simpleBufferData sb

unpackb :: OBJECT a => ByteString -> IO (Result a)
unpackb bs = do
  withZone $ \z -> do
    r <- unpackObject z bs
    return $ case r of
      Left err -> Left (show err)
      Right (_, dat) -> fromObject dat

packb' :: OBJECT a => a -> ByteString
packb' dat = unsafePerformIO $ packb dat

unpackb' :: OBJECT a => ByteString -> Result a
unpackb' bs = unsafePerformIO $ unpackb bs
