#! /usr/bin/env bash

mkdir -p submission/data

cp -r src submission
cp -r milestones submission
cp Makefile submission

zip -r submission.zip submission

scp -i ../SwDev.pem unzip.sh ubuntu@$1:~
scp -i ../SwDev.pem submission.zip ubuntu@$1:~

rm -rf submission submission.zip

