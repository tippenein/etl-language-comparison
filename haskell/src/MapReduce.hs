{-# LANGUAGE OverloadedStrings #-}
module MapReduce where

import           Control.Monad        (void)
import qualified Data.Map.Strict      as Map
import qualified Data.Text            as T
import           Pipes
import           Pipes.Prelude        as P
import           Pipes.Prelude.Text   as Text
import           Pipes.Safe
import qualified Pipes.Text           as Text
import qualified Pipes.Text.IO        as Text
import           System.FilePath.Glob
import           System.IO

-- doIt :: IO ()
-- doIt = printMap =<< doPipe

doPipe :: IO (Map.Map Text.Text Int)
doPipe = runEffect hoodProducer

foldIntoMap :: (MonadSafe m) => Producer Text.Text m () -> Map.Map Text.Text Int
foldIntoMap = P.fold incHood Map.empty id
  where
    incHood m hood = Map.insertWith (+) hood 1 m

hoodProducer :: Effect IO (Map.Map Text.Text Int)
hoodProducer =
          getInputFiles
      >-> mapper
      >-> foldIntoMap

getInputFiles :: (MonadSafe m) => Producer Text.Text m Text.Text
getInputFiles = do
  globbed <- liftIO $ globDir1 (compile "tweets_*") "../tmp/tweets"
  for (each globbed) Text.readFileLn

mapper :: (MonadSafe m) => Pipe Text.Text Text.Text m Text.Text
mapper = P.filter (\line -> "knicks" `T.isInfixOf` line)
     >-> P.map (second . T.split ((==) '\t'))

printMap :: Map.Map Text.Text Int -> ()
printMap = void $ Map.traverseWithKey (\k v -> Prelude.print (k,v))

-- writeFileWith res = Text.writeFile "../tmp/haskell_output" totals
-- writeToFile p = p >~ openFile "something.txt" WriteMode >>= P.toHandle

second :: [a] -> a
second (_:y:_) = y
second _ = error "this list ain't right"

-- Map.insertWith :: Ord k => (a -> a -> a) -> k -> a -> Map k a -> Map k a
-- P.fold :: Monad m => (x -> a -> x) -> x -> (x -> b) -> Producer a m () -> m b

      -- >-> P.take 5
      -- >-> Text.stdout
