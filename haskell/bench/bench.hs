module Main where

import Criterion.Main
import MapReduce

main = defaultMain [
  bgroup "main"
    [
      bench "all"   $ nfIO (doIt Nothing)
    , bench "25000" $ nfIO (doIt (Just 25000))
    , bench "2500"  $ nfIO (doIt (Just 2500))
    ]
  ]
