#!/bin/bash
make
rsync -P -e 'ssh -p 10022' ./filebeat bichangshuo@version.plaso.cn:/home/bichangshuo/docker_work/elk/filebeat/native
ssh -p 10022 bichangshuo@version.plaso.cn 'cd ~/docker_work/elk/filebeat/native && rm -rf data/* && rm -rf logs/* && rm -rf out/xxx/*'