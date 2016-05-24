module Main where

import MapReduce
import System.Environment

main = do
  result <- doIt Nothing
  print result
