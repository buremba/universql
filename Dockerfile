FROM public.ecr.aws/lambda/python:3.11

RUN pip install 'poetry==1.8.3'
COPY pyproject.toml poetry.lock ${LAMBDA_TASK_ROOT}

# Needed to save time and avoid build issues in Lambda
RUN poetry config virtualenvs.create false --local
# Install the dependencies first, so that we can cache them.
RUN poetry install --no-dev

# Copy everything. (Note: If needed, we can use .dockerignore to limit what's copied.)
COPY . ${LAMBDA_TASK_ROOT}

# Install again, now that we've copied the jinjat package files. Otherwise,
# Jinjat itself won't be installed.
RUN poetry install

EXPOSE 8084
ENV UNIVERSQL_HOST=0.0.0.0
ENTRYPOINT ["poetry", "run", "universql"]

