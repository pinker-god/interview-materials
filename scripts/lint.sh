#!/usr/bin/env bash

# set -ex

green() {
  echo -e "\033[0;32m$@\033[m"
}

red() {
  echo -e "\033[0;31m$@\033[m"
}

fail() {
  red "$@" 1>&2
  exit 1
}

if ! [ -x "$(command -v markdownlint)" ]; then
  if [ -x "$(command -v yarn)" ]; then
    echo 'Installing markdownlint-cli with yarn'
    yarn global add markdownlint-cli
  elif [ -x "$(command -v npm)" ]; then
    echo 'Installing markdownlint-cli with npm'
    npm install -g markdownlint-cli
  else
    fail 'Error: both yarn and npm are not installed'
  fi
fi

markdownlint .

if [ $? -ne 0 ]; then
  fail 'Error: markdownlint'
else
  green 'Finish: markdownlint'
fi
