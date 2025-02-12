#!/bin/bash

apt-get update
apt-get install -y python3-pip r-base-core libxml2-dev libfontconfig1-dev libfreetype6-dev libharfbuzz-dev libfribidi-dev libpng-dev libtiff5-dev libjpeg-dev libcurl4-openssl-dev libssl-dev libgit2-dev libsodium-dev libcairo2-dev libxt-dev

pip3 install -r requirements.txt

Rscript requirements.R
