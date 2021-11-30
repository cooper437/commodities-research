#!/bin/zsh
# Rename all file extensions in the cwd from .txt to .csv
for file in *.txt
do
  mv "$file" "${file%.txt}.csv"
done
