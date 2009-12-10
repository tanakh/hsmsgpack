module Data.MessagePack.Monad(
  ) where

import Control.Monad.Trans
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import System.IO

import Data.MessagePack.Base
import Data.MessagePack.Class

class Monad m => MonadUnpacker m where
  get :: OBJECT a => m a

type Unpacker = UnpackerT IO

newtype UnpackerT m r = UnpackerT { runUnpackerT :: Feeder -> m r }

type Feeder = IO (Maybe ByteString)

instance MonadTrans UnpackerT where
  lift m = UnpackerT $ const m

instance MonadIO m => MonadIO (UnpackerT m) where
  liftIO iom = UnpackerT $ const iom

{-
instance MonadIO m => MonadUnpacker (UnpackerT m r) where
  get = fromObject ObjectNil

unpackFromHandle :: MonadIO m => Handle -> UnpackerT m r -> m r
unpackFromHandle h m =
  runUnpackerT
-}
