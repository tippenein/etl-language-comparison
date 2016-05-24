{-# LANGUAGE OverloadedStrings #-}
module MapReduce where

import Control.Monad (void)
import qualified Data.Map.Strict as Map
import Data.Monoid ((<>))
import qualified Data.Text as T
import Pipes
import Pipes.Prelude as P
import Pipes.Prelude.Text as Text
import Pipes.Safe
import qualified Pipes.Text as Text
import qualified Pipes.Text.IO as Text
import System.FilePath.Glob
import System.IO

type HoodMap = Map.Map Text.Text Integer

showHood :: HoodMap -> T.Text
showHood = Map.foldl' aggr ""
  where
    aggr :: T.Text -> Integer -> T.Text
    aggr k v = " - " <> k <> " - " <> "\n"

doIt :: Maybe Int -> IO T.Text
doIt amount = do
  hmap <- runSafeT $ foldIntoMap $ hoodProducer amount
  return $ showHood hmap

foldIntoMap :: (MonadSafe m) => Producer Text.Text m () -> m HoodMap
foldIntoMap = P.fold incHood (Map.empty :: HoodMap) id
  where incHood m hood = Map.insertWith (+) hood 1 m

hoodProducer :: (MonadSafe m) => Maybe Int -> Producer Text.Text m ()
hoodProducer amount =
  case amount of
    Nothing -> getInputFiles >-> mapper
    Just n -> getInputFiles >-> mapper >-> P.take n

getInputFiles :: (MonadSafe m) => Producer Text.Text m ()
getInputFiles = do
  globbed <- liftIO $ globDir1 (compile "tweets_*") "../tmp/tweets"
  for (each globbed) Text.readFileLn

mapper :: (MonadSafe m) => Pipe Text.Text Text.Text m ()
mapper = P.filter (\line -> "knicks" `T.isInfixOf` line)
     >-> P.map (T.takeWhile (/= '\t') . T.dropWhile (/= '\t'))

-- writeFileWith res = Text.writeFile "../tmp/haskell_output" totals
-- writeToFile p = p >~ openFile "something.txt" WriteMode >>= P.toHandle
