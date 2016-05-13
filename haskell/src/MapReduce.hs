{-# LANGUAGE OverloadedStrings #-}
module MapReduce where

import           Pipes
import           Pipes.Prelude        as Pipes
import           Pipes.Prelude.Text   as Text
import           Pipes.Safe
import qualified Pipes.Text           as Text
import qualified Pipes.Text.IO        as Text
import           System.FilePath.Glob

doIt :: IO ()
doIt = runSafeT $ runEffect $
        all_files
    >-> filter_and_group
    -- >-> sort_by_freq_and_hood
    >-> Text.stdout

all_files = do
  globbed <- liftIO $ globDir1 (compile "tweets_*") "../tmp/tweets"
  for (each globbed) Text.readFileLn

filter_and_group = undefined
  -- Pipes.filter (\a -> any ((==) "knicks") a)
sort_by_freq_and_hood = undefined
