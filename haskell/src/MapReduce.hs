{-# LANGUAGE OverloadedStrings #-}

module MapReduce where

import Data.Foldable (traverse_)
import System.FilePath ((</>))
import System.FilePath.Glob (compile, globDir1)

import qualified Control.Concurrent.Async as Async
import qualified Control.Concurrent.STM as STM
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Lazy as ByteString.Lazy
import qualified Data.ByteString.Lazy.Char8 as ByteString.Lazy.Char8
import qualified Data.ByteString.Search as Search
import qualified ListT
import qualified STMContainers.Map as Map
import qualified System.Directory as Directory

parseFile :: ByteString.Lazy.ByteString -> [ByteString.Lazy.ByteString]
parseFile bytes0 =
    [ neighborhood line
    | line <- ByteString.Lazy.Char8.lines bytes0
    , match line
    ]
  where
    lower w8 = if 65 <= w8 && w8 < 91 then w8 + 32 else w8

    match =
          not
        . null
        . Search.indices "knicks"
        . ByteString.map lower
        . ByteString.Lazy.toStrict

    neighborhood =
          ByteString.Lazy.takeWhile (/= 9)
        . ByteString.Lazy.drop 1
        . ByteString.Lazy.dropWhile (/= 9)

main :: IO ()
main = do
    files <- globDir1 (compile "tweets_*") "../tmp/tweets"

    m <- Map.newIO

    let increment key = STM.atomically (do
            x <- Map.lookup key m
            case x of
                Nothing -> Map.insert 1 key m
                Just n  -> n' `seq` Map.insert n' key m  where n' = n + 1 )

    let processFile file = Async.Concurrently (do
            bytes <- ByteString.Lazy.readFile file

            traverse_ increment (parseFile bytes) )

    Async.runConcurrently (traverse_ processFile files)

    kvs <- STM.atomically (ListT.toList (Map.stream m))
    traverse_ print kvs
