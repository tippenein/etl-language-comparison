module Main where

import Criterion.Main
import MapReduce

main = defaultMain [
  bgroup "main"
    [
      bench "all"   $ nfIO doIt
    ]
  ]
