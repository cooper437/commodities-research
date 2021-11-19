#!/bin/zsh
# Add a csv header to files in cwd
# Note that this assumes gsed is installed which is gnu sed since os x comes with a customized sed variant
for file in $(find . -name "*.csv"); do
  echo Processing $file
  gsed -i '1s/^/DateTime,Open,High,Low,Close,Volume\n/' $file
done
