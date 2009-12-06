{-# LANGUAGE CPP #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE IncoherentInstances #-}

module Data.MessagePack.Base(
  SimpleBuffer,
  newSimpleBuffer,
  simpleBufferData,
  
  Packer,
  newPacker,
  
  packU8,
  packU16,
  packU32,
  packU64,  
  packS8,
  packS16,
  packS32,
  packS64,
  
  packTrue,
  packFalse,
  
  packInt,
  packDouble,
  packNil,
  packBool,
  
  packArray,
  packMap,
  packRAW,
  packRAWBody,
  packRAW',
  
  Object(..),
  packObject,
  
  unpackObject,
  
  Unpacker,
  defaultInitialBufferSize,
  newUnpacker,
  unpackerReserveBuffer,
  unpackerBuffer,
  unpackerBufferCapacity,
  unpackerBufferConsumed,
  unpackerFeed,
  unpackerExecute,
  unpackerData,
  unpackerReleaseZone,
  unpackerResetZone,
  unpackerReset,
  unpackerMessageSize,
  
  Zone,
  newZone,
  freeZone,
  withZone,
  ) where

import Control.Exception
import Control.Monad
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS hiding (pack, unpack)
import Data.Int
import Data.Word
import Foreign.C
import Foreign.Concurrent
import Foreign.ForeignPtr hiding (newForeignPtr)
import Foreign.Marshal.Alloc
import Foreign.Marshal.Array
import Foreign.Ptr
import Foreign.Storable

#include <msgpack.h>

type SimpleBuffer = ForeignPtr ()

type WriteCallback = Ptr () -> CString -> CUInt -> IO CInt

newSimpleBuffer :: IO SimpleBuffer
newSimpleBuffer = do
  ptr <- mallocBytes (#size msgpack_sbuffer)
  fptr <- newForeignPtr ptr $ do
    msgpack_sbuffer_destroy ptr
    free ptr
  withForeignPtr fptr $ \p ->
    msgpack_sbuffer_init p
  return fptr

simpleBufferData :: SimpleBuffer -> IO ByteString
simpleBufferData sb =
  withForeignPtr sb $ \ptr -> do
    size <- (#peek msgpack_sbuffer, size) ptr
    dat  <- (#peek msgpack_sbuffer, data) ptr
    BS.packCStringLen (dat, fromIntegral (size :: CSize))

#def void msgpack_sbuffer_init_wrap(msgpack_sbuffer* sbuf){ msgpack_sbuffer_init(sbuf); }

foreign import ccall "msgpack_sbuffer_init_wrap" msgpack_sbuffer_init ::
  Ptr () -> IO ()

#def void msgpack_sbuffer_destroy_wrap(msgpack_sbuffer* sbuf){ msgpack_sbuffer_destroy(sbuf); }

foreign import ccall "msgpack_sbuffer_destroy_wrap" msgpack_sbuffer_destroy ::
  Ptr () -> IO ()

#def int msgpack_sbuffer_write_wrap(void* data, const char* buf, unsigned int len){ return msgpack_sbuffer_write(data, buf, len); }
foreign import ccall "msgpack_sbuffer_write_wrap" msgpack_sbuffer_write ::
  WriteCallback

type Packer = ForeignPtr ()

newPacker :: SimpleBuffer -> IO Packer
newPacker sbuf = do
  cb <- wrap_callback msgpack_sbuffer_write
  ptr <- withForeignPtr sbuf $ \ptr ->
    msgpack_packer_new ptr cb
  fptr <- newForeignPtr ptr $ do
    msgpack_packer_free ptr
  return fptr

#def msgpack_packer* msgpack_packer_new_wrap(void *data, msgpack_packer_write callback){ return msgpack_packer_new(data, callback); }

foreign import ccall "msgpack_packer_new_wrap" msgpack_packer_new ::
  Ptr () -> FunPtr WriteCallback -> IO (Ptr ())

#def void msgpack_packer_free_wrap(msgpack_packer* pk){ msgpack_packer_free(pk); }

foreign import ccall "msgpack_packer_free_wrap" msgpack_packer_free ::
  Ptr () -> IO ()

foreign import ccall "wrapper" wrap_callback ::
  WriteCallback -> IO (FunPtr WriteCallback)

packU8 :: Packer -> Word8 -> IO Int
packU8 pc n =
  liftM fromIntegral $ withForeignPtr pc $ \ptr ->
    msgpack_pack_uint8 ptr n

#def int msgpack_pack_uint8_wrap(msgpack_packer* pk, uint8_t d){ return msgpack_pack_uint8(pk, d); }
foreign import ccall "msgpack_pack_uint8_wrap" msgpack_pack_uint8 ::
  Ptr () -> Word8 -> IO CInt

packU16 :: Packer -> Word16 -> IO Int
packU16 pc n =
  liftM fromIntegral $ withForeignPtr pc $ \ptr ->
    msgpack_pack_uint16 ptr n

#def int msgpack_pack_uint16_wrap(msgpack_packer* pk, uint16_t d){ return msgpack_pack_uint16(pk, d); }
foreign import ccall "msgpack_pack_uint16_wrap" msgpack_pack_uint16 ::
  Ptr () -> Word16 -> IO CInt

packU32 :: Packer -> Word32 -> IO Int
packU32 pc n =
  liftM fromIntegral $ withForeignPtr pc $ \ptr ->
    msgpack_pack_uint32 ptr n

#def int msgpack_pack_uint32_wrap(msgpack_packer* pk, uint32_t d){ return msgpack_pack_uint32(pk, d); }
foreign import ccall "msgpack_pack_uint32_wrap" msgpack_pack_uint32 ::
  Ptr () -> Word32 -> IO CInt

packU64 :: Packer -> Word64 -> IO Int
packU64 pc n =
  liftM fromIntegral $ withForeignPtr pc $ \ptr ->
    msgpack_pack_uint64 ptr n

#def int msgpack_pack_uint64_wrap(msgpack_packer* pk, uint64_t d){ return msgpack_pack_uint64(pk, d); }
foreign import ccall "msgpack_pack_uint64_wrap" msgpack_pack_uint64 ::
  Ptr () -> Word64 -> IO CInt

packS8 :: Packer -> Int8 -> IO Int
packS8 pc n =
  liftM fromIntegral $ withForeignPtr pc $ \ptr ->
    msgpack_pack_int8 ptr n

#def int msgpack_pack_int8_wrap(msgpack_packer* pk, int8_t d){ return msgpack_pack_int8(pk, d); }
foreign import ccall "msgpack_pack_int8_wrap" msgpack_pack_int8 ::
  Ptr () -> Int8 -> IO CInt

packS16 :: Packer -> Int16 -> IO Int
packS16 pc n =
  liftM fromIntegral $ withForeignPtr pc $ \ptr ->
    msgpack_pack_int16 ptr n

#def int msgpack_pack_int16_wrap(msgpack_packer* pk, int16_t d){ return msgpack_pack_int16(pk, d); }
foreign import ccall "msgpack_pack_int16_wrap" msgpack_pack_int16 ::
  Ptr () -> Int16 -> IO CInt

packS32 :: Packer -> Int32 -> IO Int
packS32 pc n =
  liftM fromIntegral $ withForeignPtr pc $ \ptr ->
    msgpack_pack_int32 ptr n

#def int msgpack_pack_int32_wrap(msgpack_packer* pk, int32_t d){ return msgpack_pack_int32(pk, d); }
foreign import ccall "msgpack_pack_int32_wrap" msgpack_pack_int32 ::
  Ptr () -> Int32 -> IO CInt

packS64 :: Packer -> Int64 -> IO Int
packS64 pc n =
  liftM fromIntegral $ withForeignPtr pc $ \ptr ->
    msgpack_pack_int64 ptr n

#def int msgpack_pack_int64_wrap(msgpack_packer* pk, int64_t d){ return msgpack_pack_int64(pk, d); }
foreign import ccall "msgpack_pack_int64_wrap" msgpack_pack_int64 ::
  Ptr () -> Int64 -> IO CInt

packInt :: Integral a => Packer -> a -> IO Int
packInt pc n = packS64 pc $ fromIntegral n

packDouble :: Packer -> Double -> IO Int
packDouble pc d =
  liftM fromIntegral $ withForeignPtr pc $ \ptr ->
    msgpack_pack_double ptr (realToFrac d)

#def int msgpack_pack_double_wrap(msgpack_packer* pk, double d){ return msgpack_pack_double(pk, d); }
foreign import ccall "msgpack_pack_double_wrap" msgpack_pack_double ::
  Ptr () -> CDouble -> IO CInt

packNil :: Packer -> IO Int
packNil pc =
  liftM fromIntegral $ withForeignPtr pc $ \ptr ->
    msgpack_pack_nil ptr

#def int msgpack_pack_nil_wrap(msgpack_packer* pk){ return msgpack_pack_nil(pk); }
foreign import ccall "msgpack_pack_nil_wrap" msgpack_pack_nil ::
  Ptr () -> IO CInt

packTrue :: Packer -> IO Int
packTrue pc =
  liftM fromIntegral $ withForeignPtr pc $ \ptr ->
    msgpack_pack_true ptr

#def int msgpack_pack_true_wrap(msgpack_packer* pk){ return msgpack_pack_true(pk); }
foreign import ccall "msgpack_pack_true_wrap" msgpack_pack_true ::
  Ptr () -> IO CInt

packFalse :: Packer -> IO Int
packFalse pc =
  liftM fromIntegral $ withForeignPtr pc $ \ptr ->
    msgpack_pack_false ptr

#def int msgpack_pack_false_wrap(msgpack_packer* pk){ return msgpack_pack_false(pk); }
foreign import ccall "msgpack_pack_false_wrap" msgpack_pack_false ::
  Ptr () -> IO CInt

packBool :: Packer -> Bool -> IO Int
packBool pc True  = packTrue pc
packBool pc False = packFalse pc

packArray :: Packer -> Int -> IO Int
packArray pc n =
  liftM fromIntegral $ withForeignPtr pc $ \ptr ->
    msgpack_pack_array ptr (fromIntegral n)

#def int msgpack_pack_array_wrap(msgpack_packer* pk, unsigned int n){ return msgpack_pack_array(pk, n); }
foreign import ccall "msgpack_pack_array_wrap" msgpack_pack_array ::
  Ptr () -> CUInt -> IO CInt

packMap :: Packer -> Int -> IO Int
packMap pc n =
  liftM fromIntegral $ withForeignPtr pc $ \ptr ->
    msgpack_pack_map ptr (fromIntegral n)

#def int msgpack_pack_map_wrap(msgpack_packer* pk, unsigned int n){ return msgpack_pack_map(pk, n); }
foreign import ccall "msgpack_pack_map_wrap" msgpack_pack_map ::
  Ptr () -> CUInt -> IO CInt

packRAW :: Packer -> Int -> IO Int
packRAW pc n =
  liftM fromIntegral $ withForeignPtr pc $ \ptr ->
    msgpack_pack_raw ptr (fromIntegral n)

#def int msgpack_pack_raw_wrap(msgpack_packer* pk, size_t l){ return msgpack_pack_raw(pk, l); }
foreign import ccall "msgpack_pack_raw_wrap" msgpack_pack_raw ::
  Ptr () -> CSize -> IO CInt

packRAWBody :: Packer -> ByteString -> IO Int
packRAWBody pc bs =
  liftM fromIntegral $ withForeignPtr pc $ \ptr ->
  BS.useAsCStringLen bs $ \(str, len) ->
    msgpack_pack_raw_body ptr (castPtr str) (fromIntegral len)

#def int msgpack_pack_raw_body_wrap(msgpack_packer* pk, const void *b, size_t l){ return msgpack_pack_raw_body(pk, b, l); }
foreign import ccall "msgpack_pack_raw_body_wrap" msgpack_pack_raw_body ::
  Ptr () -> Ptr () -> CSize -> IO CInt

packRAW' :: Packer -> ByteString -> IO Int
packRAW' pc bs = do
  packRAW pc (BS.length bs)
  packRAWBody pc bs

type Unpacker = ForeignPtr ()

defaultInitialBufferSize :: Int
defaultInitialBufferSize = 32 * 1024 -- #const MSGPACK_UNPACKER_DEFAULT_INITIAL_BUFFER_SIZE

newUnpacker :: Int -> IO Unpacker
newUnpacker initialBufferSize = do
  ptr <- msgpack_unpacker_new (fromIntegral initialBufferSize)
  fptr <- newForeignPtr ptr $ do
    msgpack_unpacker_free ptr
  return fptr

-- #def msgpack_unpacker *msgpack_unpacker_new_wrap(size_t initial_buffer_size){ return msgpack_unpacker_new(initial_buffer_size); }

foreign import ccall "msgpack_unpacker_new" msgpack_unpacker_new ::
  CSize -> IO (Ptr ())

foreign import ccall "msgpack_unpacker_free" msgpack_unpacker_free ::
  Ptr() -> IO ()

packObject :: Packer -> Object -> IO ()
packObject pc ObjectNil = packNil pc >> return ()

packObject pc (ObjectBool b) = packBool pc b >> return ()

packObject pc (ObjectInteger n) = packInt pc n >> return ()

packObject pc (ObjectDouble d) = packDouble pc d >> return ()

packObject pc (ObjectRAW bs) = packRAW' pc bs >> return ()

packObject pc (ObjectArray ls) = do
  packArray pc (length ls)
  mapM_ (packObject pc) ls

packObject pc (ObjectMap ls) = do
  packMap pc (length ls)
  mapM_ (\(a, b) -> packObject pc a >> packObject pc b) ls

data UnpackReturn =
  UnpackContinue
  | UnpackParseError
  | UnpackError
  deriving (Eq, Show)

unpackObject :: Zone -> ByteString -> IO (Either UnpackReturn (Int, Object))
unpackObject z dat =
  allocaBytes (#size msgpack_object) $ \ptr ->
  BS.useAsCStringLen dat $ \(str, len) ->
  alloca $ \poff -> do
    ret <- msgpack_unpack str (fromIntegral len) poff z ptr
    case ret of
      (#const MSGPACK_UNPACK_SUCCESS) -> do
        off <- peek poff
        obj <- peekObject ptr
        return $ Right (fromIntegral off, obj)
      (#const MSGPACK_UNPACK_EXTRA_BYTES) -> do
        off <- peek poff
        obj <- peekObject ptr
        return $ Right (fromIntegral off, obj)
      (#const MSGPACK_UNPACK_CONTINUE) ->
        return $ Left UnpackContinue
      (#const MSGPACK_UNPACK_PARSE_ERROR) ->
        return $ Left UnpackParseError
      _ ->
        return $ Left UnpackError

foreign import ccall "msgpack_unpack" msgpack_unpack ::
  Ptr CChar -> CSize -> Ptr CSize -> Zone -> Ptr () -> IO CInt

unpackerReserveBuffer :: Unpacker -> Int -> IO Bool
unpackerReserveBuffer up size =
  withForeignPtr up $ \ptr ->
  liftM (/=0) $ msgpack_unpacker_reserve_buffer ptr (fromIntegral size)

#def bool msgpack_unpacker_reserve_buffer_wrap(msgpack_unpacker *mpac, size_t size){ return msgpack_unpacker_reserve_buffer(mpac, size); }

foreign import ccall "msgpack_unpacker_reserve_buffer_wrap" msgpack_unpacker_reserve_buffer ::
  Ptr () -> CSize -> IO CChar

unpackerBuffer :: Unpacker -> IO (Ptr CChar)
unpackerBuffer up =
  withForeignPtr up $ \ptr ->
  msgpack_unpacker_buffer ptr

#def char *msgpack_unpacker_buffer_wrap(msgpack_unpacker *mpac){ return msgpack_unpacker_buffer(mpac); }

foreign import ccall "msgpack_unpacker_buffer_wrap" msgpack_unpacker_buffer ::
  Ptr () -> IO (Ptr CChar)

unpackerBufferCapacity :: Unpacker -> IO Int
unpackerBufferCapacity up =
  withForeignPtr up $ \ptr ->
  liftM fromIntegral $ msgpack_unpacker_buffer_capacity ptr

#def size_t msgpack_unpacker_buffer_capacity_wrap(const msgpack_unpacker *mpac){ return msgpack_unpacker_buffer_capacity(mpac); }

foreign import ccall "msgpack_unpacker_buffer_capacity_wrap" msgpack_unpacker_buffer_capacity ::
  Ptr () -> IO CSize

unpackerBufferConsumed :: Unpacker -> Int -> IO ()
unpackerBufferConsumed up size =
  withForeignPtr up $ \ptr ->
  msgpack_unpacker_buffer_consumed ptr (fromIntegral size)

#def void msgpack_unpacker_buffer_consumed_wrap(msgpack_unpacker *mpac, size_t size){ msgpack_unpacker_buffer_consumed(mpac, size); }

foreign import ccall "msgpack_unpacker_buffer_consumed_wrap" msgpack_unpacker_buffer_consumed ::
  Ptr () -> CSize -> IO ()

unpackerFeed :: Unpacker -> ByteString -> IO ()
unpackerFeed up bs =
  BS.useAsCStringLen bs $ \(str, len) -> do
    True <- unpackerReserveBuffer up len
    ptr <- unpackerBuffer up
    copyArray ptr str len
    unpackerBufferConsumed up len

unpackerExecute :: Unpacker -> IO Int
unpackerExecute up =
  withForeignPtr up $ \ptr ->
  liftM fromIntegral $ msgpack_unpacker_execute ptr

foreign import ccall "msgpack_unpacker_execute" msgpack_unpacker_execute ::
  Ptr () -> IO CInt

unpackerData :: Unpacker -> IO Object
unpackerData up =
  withForeignPtr up $ \ptr ->
  allocaBytes (#size msgpack_object) $ \pobj -> do
    msgpack_unpacker_data ptr pobj
    peekObject pobj

#def void msgpack_unpacker_data_wrap(msgpack_unpacker *mpac, msgpack_object *obj){ *obj=msgpack_unpacker_data(mpac); }

foreign import ccall "msgpack_unpacker_data_wrap" msgpack_unpacker_data ::
  Ptr () -> Ptr () -> IO ()

unpackerReleaseZone :: Unpacker -> IO Zone
unpackerReleaseZone up =
  withForeignPtr up $ \ptr ->
  msgpack_unpacker_release_zone ptr

foreign import ccall "msgpack_unpacker_release_zone" msgpack_unpacker_release_zone ::
  Ptr () -> IO (Ptr ())

unpackerResetZone :: Unpacker -> IO ()
unpackerResetZone up =
  withForeignPtr up $ \ptr ->
  msgpack_unpacker_reset_zone ptr

foreign import ccall "msgpack_unpacker_reset_zone" msgpack_unpacker_reset_zone ::
  Ptr () -> IO ()

unpackerReset :: Unpacker -> IO ()
unpackerReset up =
  withForeignPtr up $ \ptr ->
  msgpack_unpacker_reset ptr

foreign import ccall "msgpack_unpacker_reset" msgpack_unpacker_reset ::
  Ptr () -> IO ()

unpackerMessageSize :: Unpacker -> IO Int
unpackerMessageSize up =
  withForeignPtr up $ \ptr ->
  liftM fromIntegral $ msgpack_unpacker_message_size ptr

#def size_t msgpack_unpacker_message_size_wrap(const msgpack_unpacker *mpac){ return msgpack_unpacker_message_size(mpac); }

foreign import ccall "msgpack_unpacker_message_size_wrap" msgpack_unpacker_message_size ::
  Ptr () -> IO CSize

type Zone = Ptr ()

newZone :: IO Zone
newZone =
  msgpack_zone_new (#const MSGPACK_ZONE_CHUNK_SIZE)

freeZone :: Zone -> IO ()
freeZone z =
  msgpack_zone_free z

withZone :: (Zone -> IO a) -> IO a
withZone z =
  bracket newZone freeZone z

foreign import ccall "msgpack_zone_new" msgpack_zone_new ::
  CSize -> IO Zone

foreign import ccall "msgpack_zone_free" msgpack_zone_free ::
  Zone -> IO ()

data Object =
  ObjectNil
  | ObjectBool Bool
  | ObjectInteger Int
  | ObjectDouble Double
  | ObjectRAW ByteString
  | ObjectArray [Object]
  | ObjectMap [(Object, Object)]
  deriving (Show)

peekObject :: Ptr a -> IO Object
peekObject ptr = do
  typ <- (#peek msgpack_object, type) ptr
  case (typ :: CInt) of
    (#const MSGPACK_OBJECT_NIL) ->
      return ObjectNil
    (#const MSGPACK_OBJECT_BOOLEAN) ->
      peekObjectBool ptr
    (#const MSGPACK_OBJECT_POSITIVE_INTEGER) ->
      peekObjectPositiveInteger ptr
    (#const MSGPACK_OBJECT_NEGATIVE_INTEGER) ->
      peekObjectNegativeInteger ptr
    (#const MSGPACK_OBJECT_DOUBLE) ->
      peekObjectDouble ptr
    (#const MSGPACK_OBJECT_RAW) ->
      peekObjectRAW ptr
    (#const MSGPACK_OBJECT_ARRAY) ->
      peekObjectArray ptr
    (#const MSGPACK_OBJECT_MAP) ->
      peekObjectMap ptr
    _ ->
      fail "peekObject: unknown object type"

peekObjectBool :: Ptr a -> IO Object
peekObjectBool ptr = do
  b <- (#peek msgpack_object, via.boolean) ptr
  return $ ObjectBool $ (b :: CUChar) /= 0

peekObjectPositiveInteger :: Ptr a -> IO Object
peekObjectPositiveInteger ptr = do
  n <- (#peek msgpack_object, via.u64) ptr
  return $ ObjectInteger $ fromIntegral (n :: Word64)

peekObjectNegativeInteger :: Ptr a -> IO Object
peekObjectNegativeInteger ptr = do
  n <- (#peek msgpack_object, via.i64) ptr
  return $ ObjectInteger $ fromIntegral (n :: Int64)

peekObjectDouble :: Ptr a -> IO Object
peekObjectDouble ptr = do
  d <- (#peek msgpack_object, via.dec) ptr
  return $ ObjectDouble $ realToFrac (d :: CDouble)

peekObjectRAW :: Ptr a -> IO Object
peekObjectRAW ptr = do
  size <- (#peek msgpack_object, via.raw.size) ptr
  p    <- (#peek msgpack_object, via.raw.ptr) ptr
  bs   <- BS.packCStringLen (p, fromIntegral (size :: Word32))
  return $ ObjectRAW bs

peekObjectArray :: Ptr a -> IO Object
peekObjectArray ptr = do
  size <- (#peek msgpack_object, via.array.size) ptr
  p    <- (#peek msgpack_object, via.array.ptr) ptr
  objs <- mapM (\i -> peekObject $ p `plusPtr`
                      ((#size msgpack_object) * i))
          [0..size-1]
  return $ ObjectArray objs

peekObjectMap :: Ptr a -> IO Object
peekObjectMap ptr = do
  size <- (#peek msgpack_object, via.map.size) ptr
  p    <- (#peek msgpack_object, via.map.ptr) ptr
  dat  <- mapM (\i -> peekObjectKV $ p `plusPtr`
                      ((#size msgpack_object_kv) * i))
          [0..size-1]
  return $ ObjectMap dat

peekObjectKV :: Ptr a -> IO (Object, Object)
peekObjectKV ptr = do
  k <- peekObject $ ptr `plusPtr` (#offset msgpack_object_kv, key)
  v <- peekObject $ ptr `plusPtr` (#offset msgpack_object_kv, val)
  return (k, v)
