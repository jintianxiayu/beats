#!/bin/bash
make
rsync -P -e 'ssh -p 10022' ./filebeat bichangshuo@version.plaso.cn:/home/bichangshuo/script/dockerfiles/filebeat/7.13.1.2/filebeat-7.13.1-linux-x86_64
# ssh -p 10022 bichangshuo@version.plaso.cn 'cd ~/docker_work/elk/filebeat/native && rm -rf data/* && rm -rf logs/* && rm -rf out/xxx/*'