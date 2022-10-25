FROM nvidia/cuda:11.3.1-devel-ubuntu20.04 as runtime
# When image is run, run the code with the environment
# activated:
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install ffmpeg libsm6 libxext6 gcc g++ curl python3 python3-distutils python3-dev python3-pip libpq-dev -y && \
	python3 -m pip install -U pip && \
	python3 -m pip install faust-streaming psycopg2 numpy websockets

COPY . .
RUN chmod a+x entry.sh
SHELL ["/bin/bash", "-c"]
ENTRYPOINT ["./entry.sh"]
