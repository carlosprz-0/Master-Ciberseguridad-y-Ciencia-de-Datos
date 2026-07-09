FROM ubuntu:latest

RUN apt update && apt upgrade -y

RUN apt install -y \
  build-essential \
  cmake \
  git \
  libgtest-dev \
  libssl-dev \
  libcrypto++-dev \
  libcrypto++-doc \
  libcrypto++-utils


RUN apt install astyle cmake gcc ninja-build libssl-dev python3-pytest python3-pytest-xdist unzip xsltproc doxygen graphviz python3-yaml valgrind -y
RUN  git clone -b main https://github.com/open-quantum-safe/liboqs.git
WORKDIR /liboqs
RUN mkdir build && cd build
WORKDIR /liboqs/build
RUN cmake -GNinja ..
RUN ninja
RUN ninja install

WORKDIR /app

COPY . .

WORKDIR /app

