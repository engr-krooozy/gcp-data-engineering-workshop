# Dockerfile for Dataflow Flex Template
FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

# Set the working directory
WORKDIR /template

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the pipeline source code
COPY pipeline.py .

# Set the entrypoint for the template launcher
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/pipeline.py
