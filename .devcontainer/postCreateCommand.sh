#!/bin/bash

apt-get update
apt-get install -y pip python3 r-base-core

pip3 install -r requirements.txt

Rscript requirements.R
