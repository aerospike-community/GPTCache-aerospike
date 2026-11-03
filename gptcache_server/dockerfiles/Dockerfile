FROM python:3.8-slim-bullseye@sha256:e191a71397fd61fbddb6712cd43ef9a2c17df0b5e7ba67607128554cd6bff267

ENV PYTHONDONTWRITEBYTECODE=1

RUN pip3 install --upgrade pip

RUN pip3 install --no-cache-dir gptcache

WORKDIR /workspace

CMD ["gptcache_server", "-s", "0.0.0.0"]
