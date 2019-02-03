FROM python:3.6

RUN \
  pip3 install pandas && \
  pip3 install requests && \
  pip install gtfs-realtime-bindings