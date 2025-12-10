# Dockerfile for Dataflow Flex Template
FROM gcr.io/dataflow-templates-base/python3-template-launcher-base
WORKDIR /template
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pipeline.py .
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/pipeline.py
