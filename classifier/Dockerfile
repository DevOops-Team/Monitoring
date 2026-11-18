# 빌드(레포 루트에서): docker build -f classifier/Dockerfile -t <이름> \
#   --build-arg RULE_VERSION=$(git log -1 --format=%h -- classifier/classifier/rules.yaml) .
# 컨텍스트가 레포 루트인 이유: 발행 전 검증에 쓰는 schemas/fault-event.v1.json이 classifier/ 밖에 있다
FROM python:3.12.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app/classifier

COPY classifier/requirements.txt .
RUN pip install -r requirements.txt

# publish.py가 자기 위치에서 두 단계 위의 schemas/를 읽는다. 레포와 같은 상대 위치로 둔다
COPY schemas/fault-event.v1.json /app/schemas/fault-event.v1.json
COPY classifier/classifier/ classifier/

# 판정 표(rules.yaml)의 git SHA 앞 7자. Kafka 헤더 rule_version으로 나간다
ARG RULE_VERSION=dev
ENV RULE_VERSION=${RULE_VERSION}

RUN useradd --uid 1000 --create-home classifier
USER 1000
EXPOSE 8080

CMD ["uvicorn", "classifier.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8080", "--log-level", "warning"]
