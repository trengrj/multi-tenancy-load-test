FROM python:3.11-bullseye

RUN apt-get update && apt-get install -y \
    libhdf5-dev \
    gcc \
    wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN wget https://storage.googleapis.com/ann-datasets/custom/fiqa-12k-384-dot.hdf5
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY weaviate_client-3.21.1.dev14+g18eb192-py3-none-any.whl .
RUN pip install weaviate_client-3.21.1.dev14+g18eb192-py3-none-any.whl

COPY *.py ./
