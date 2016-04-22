{-# LANGUAGE OverloadedStrings #-}
module MapReduce where

import           Pipes
import           Pipes.Safe
import qualified Pipes.Text    as Text
import qualified Pipes.Text.IO as Text

doIt :: IO ()
doIt = runSafeT $ runEffect $
        Text.readFile "../tmp/tweets/tweets_aa"
    >-> filter_and_group
    >-> sort_by_freq_and_hood
    >-> Text.stdout

filter_and_group = undefined
sort_by_freq_and_hood = undefined
