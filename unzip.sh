#! /usr/bin/env bash

sudo apt update
sudo apt install unzip make g++

unzip submission.zip
cp submission/milestones/default_config.txt submission/config.txt
mv submission $1

rm submission.zip


