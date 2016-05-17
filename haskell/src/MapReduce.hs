{-# LANGUAGE OverloadedStrings #-}
module MapReduce where

import Control.Monad (void)
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import Pipes
import Pipes.Prelude as P
import Pipes.Prelude.Text as Text
import Pipes.Safe
import qualified Pipes.Text as Text
import qualified Pipes.Text.IO as Text
import System.FilePath.Glob
import System.IO

doIt :: IO ()
doIt = return () -- printMap =<< doPipe

-- doPipe :: ()
-- doPipe =
--   printMap =<< foldIntoMap hoodProducer

-- foldIntoMap producer = P.fold $ (\t -> Map.insertWith (+) t 1 producer) initialMap id
-- foldIntoMap producer = P.fold $ Map.insertWith initialMap (+ 1) producer

-- hoodProducer :: P.Producer Text.Text () Text.Text ()
-- hoodProducer :: Proxy () Text.Text () Text.Text (SafeT IO) r
hoodProducer :: IO ()
hoodProducer =
  runSafeT $ runEffect $
          getInputFiles
      >-> mapper
      >-> P.take 5
      -- >-> sortByFreqAndHood
      >-> Text.stdout
  -- >~ openFile "something.txt" WriteMode >>= P.toHandle

getInputFiles = do
  globbed <- liftIO $ globDir1 (compile "tweets_*") "../tmp/tweets"
  for (each globbed) Text.readFileLn

-- Producer Text.Text () [Text.Text]
type TProducer r = Proxy () Text.Text () Text.Text (SafeT IO) r

mapper :: Proxy () Text.Text () Text.Text (SafeT IO) r
mapper = P.filter (\line -> "knicks" `T.isInfixOf` line)
     >-> P.map (second . T.split ((==) '\t'))


initialMap :: Map.Map T.Text Int
initialMap = Map.empty


printMap :: Map.Map Text.Text Int -> ()
printMap = void $ Map.traverseWithKey (\k v -> Prelude.print (k,v))

-- P.fold (+) 0 id process

-- fold :: (x -> a -> x) -> x -> (x -> b) -> Producer a m () -> m b

-- writeFileWith res = Text.writeFile "../tmp/haskell_output" totals
-- foldrWithKey :: (k -> a -> b -> b) -> b -> Map k a -> b
-- sortByFreqAndHood :: Map.Map Text.Text Int -> Text.Text
-- sortByFreqAndHood = Map.foldrWithKey f ""
--   where
--     f k a result = result ++ "(" ++ Prelude.show $ Text.pack k ++ ":" ++ Prelude.show $ Text.pack a ++ ")"

second :: [a] -> a
second (_:y:_) = y
second _ = error "this list ain't right"

