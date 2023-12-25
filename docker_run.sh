docker run --name Stream4videosRTSP --gpus all -it --restart unless-stopped --network host -v /var/run/docker.sock:/var/run/docker.sock -v ~/dspip/ATR:/app/ATR -v $PWD:/app/stream4videos --device-cgroup-rule='c 81:* rmw' -v /tmp/.X11-unix:/tmp/.X11-unix -e DISPLAY=$DISPLAY atr_docker:v1
