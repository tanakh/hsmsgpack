module Data.MessagePack.Monad(
  MonadUnpacker(..),
  
  UnpackerT(..),
  
  unpackFrom,
  unpackFromHandle,
  unpackFromFile,
  unpackFromString,
  ) where

import Control.Monad
import Control.Monad.Trans
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import System.IO

import Data.MessagePack.Base hiding (Unpacker)
import qualified Data.MessagePack.Base as Base
import Data.MessagePack.Class
import Data.MessagePack.Feed

class Monad m => MonadUnpacker m where
  get :: OBJECT a => m a

newtype UnpackerT m r = UnpackerT { runUnpackerT :: Base.Unpacker -> Feeder -> m r }

instance Monad m => Monad (UnpackerT m) where
  a >>= b =
    UnpackerT $ \up feed -> do
      r <- runUnpackerT a up feed
      runUnpackerT (b r) up feed
  
  return r =
    UnpackerT $ \_ _ -> return r

instance MonadTrans UnpackerT where
  lift m = UnpackerT $ \_ _ -> m

instance MonadIO m => MonadIO (UnpackerT m) where
  liftIO = lift . liftIO

instance MonadIO m => MonadUnpacker (UnpackerT m) where
  get = UnpackerT $ \up feed -> liftIO $ do
    resp <- unpackerExecute up
    guard $ resp>=0
    when (resp==0) $ do
      Just bs <- feed
      unpackerFeed up bs
      resp2 <- unpackerExecute up
      guard $ resp2==1
    obj <- unpackerData up
    freeZone =<< unpackerReleaseZone up
    unpackerReset up
    let Right r = fromObject obj
    return r

unpackFrom :: MonadIO m => Feeder -> UnpackerT m r -> m r
unpackFrom f m = do
  up <- liftIO $ newUnpacker defaultInitialBufferSize
  runUnpackerT m up f

unpackFromHandle :: MonadIO m => Handle -> UnpackerT m r -> m r
unpackFromHandle h m =
  flip unpackFrom m =<< liftIO (feederFromHandle h)

unpackFromFile :: MonadIO m => FilePath -> UnpackerT m r -> m r
unpackFromFile p m = do
  h <- liftIO $ openFile p ReadMode
  r <- flip unpackFrom m =<< liftIO (feederFromHandle h)
  liftIO $ hClose h
  return r

unpackFromString :: MonadIO m => ByteString -> UnpackerT m r -> m r
unpackFromString bs m = do
  flip unpackFrom m =<< liftIO (feederFromString bs)
