# DCGM 엔진(nv-hostengine)과 dcgmi만 담은 이미지. dcgm-exporter Pod의 사이드카로 쓴다(deploy/agents/gpu/dcgm-exporter).
# exporter가 이 엔진을 보게 하면 dcgmi test --inject로 넣은 가짜 Xid가 exporter 지표에 나온다(내장 엔진이면 안 보임).
# 공식 nvcr.io/nvidia/cloud-native/dcgm을 쓰지 않는 이유: 진단용 CUDA 묶음까지 들어 있어 받으면 약 7.3GB를 차지한다.
#   GPU 노드 루트 디스크는 21.4GB(11/19 최소 여유 3.6GB)라 받다가 학습 Pod가 축출될 수 있다. core 패키지만이면 설치 49MB
# 빌드: docker build -t <이름> dcgm-hostengine/
FROM ubuntu:noble-20261024.1

# dcgm-exporter 4.6.1-4.8.4가 쓰는 DCGM과 같은 버전
ARG DCGM_VERSION=1:4.6.1-1
ARG CUDA_KEYRING_SHA256=d2a6b11c096396d868758b86dab1823b25e14d70333f1dfa74da5ddaf6a06dba

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && curl -fsSLo /tmp/cuda-keyring.deb https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2404/x86_64/cuda-keyring_1.1-1_all.deb \
 && echo "${CUDA_KEYRING_SHA256}  /tmp/cuda-keyring.deb" | sha256sum -c - \
 && dpkg -i /tmp/cuda-keyring.deb \
 && apt-get update \
 && apt-get install -y --no-install-recommends datacenter-gpu-manager-4-core=${DCGM_VERSION} \
 && apt-get purge -y --auto-remove curl \
 && rm -rf /var/lib/apt/lists/* /tmp/cuda-keyring.deb

# GPU는 exporter 이미지와 같은 방식으로 받는다(nvidia.com/gpu를 요청하지 않고 런타임이 전부 붙임)
ENV NVIDIA_VISIBLE_DEVICES=all NVIDIA_DRIVER_CAPABILITIES=compute,utility NVIDIA_DISABLE_REQUIRE=true

EXPOSE 5555
# 포그라운드, Pod 안(localhost)에서만 연다. 로그는 표준 출력으로(기본은 컨테이너 안 파일이라 Fluent Bit이 못 읽는다).
# GPU를 못 받아도 엔진은 「GPU 없이」 뜬다 → 정상 여부는 Running이 아니라 exporter의 GPU 지표 개수로 본다
CMD ["nv-hostengine", "--no-daemon", "--bind-interface", "127.0.0.1", "--log-filename", "/dev/stdout"]
