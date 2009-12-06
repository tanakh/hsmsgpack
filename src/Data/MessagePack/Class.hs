{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverlappingInstances #-}

module Data.MessagePack.Class(
  OBJECT(..),
  Result(..),
  pack,
  ) where

import Control.Monad.Error
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as C8
import Data.Either

import Data.MessagePack.Base

class OBJECT a where
  toObject :: a -> Object
  fromObject :: Object -> Result a

type Result a = Either String a

instance OBJECT Object where
  toObject = id
  fromObject = Right

fromObjectError :: String
fromObjectError = "fromObject: cannot cast"

instance OBJECT Int where
  toObject = ObjectInteger
  fromObject (ObjectInteger n) = Right n
  fromObject _ = Left fromObjectError

instance OBJECT Bool where
  toObject = ObjectBool
  fromObject (ObjectBool b) = Right b
  fromObject _ = Left fromObjectError

instance OBJECT Double where
  toObject = ObjectDouble
  fromObject (ObjectDouble d) = Right d
  fromObject _ = Left fromObjectError

instance OBJECT ByteString where
  toObject = ObjectRAW
  fromObject (ObjectRAW bs) = Right bs
  fromObject _ = Left fromObjectError

instance OBJECT String where
  toObject = toObject . C8.pack
  fromObject obj = liftM C8.unpack $ fromObject obj

instance OBJECT a => OBJECT [a] where
  toObject = ObjectArray . map toObject
  fromObject (ObjectArray arr) =
    mapM fromObject arr
  fromObject _ =
    Left fromObjectError

instance (OBJECT a, OBJECT b) => OBJECT [(a, b)] where
  toObject =
    ObjectMap . map (\(a, b) -> (toObject a, toObject b))
  fromObject (ObjectMap mem) = do
    mapM (\(a, b) -> liftM2 (,) (fromObject a) (fromObject b)) mem
  fromObject _ =
    Left fromObjectError

instance OBJECT a => OBJECT (Maybe a) where
  toObject (Just a) = toObject a
  toObject Nothing = ObjectNil
  
  fromObject ObjectNil = return Nothing
  fromObject obj = liftM Just $ fromObject obj

pack :: OBJECT a => Packer -> a -> IO ()
pack pc = packObject pc . toObject
