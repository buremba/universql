FROM public.ecr.aws/lambda/python:3.11

RUN yum install gcc -y

RUN pip install 'poetry==1.8.3'
COPY pyproject.toml ${LAMBDA_TASK_ROOT}

# Needed to save time and avoid build issues in Lambda
RUN poetry config virtualenvs.create false --local
# Install the dependencies first, so that we can cache them.
RUN poetry install --only main

# Copy everything. (Note: If needed, we can use .dockerignore to limit what's copied.)
COPY . ${LAMBDA_TASK_ROOT}

# Install again, now that we've copied the jinjat package files. Otherwise,
# Jinjat itself won't be installed.
RUN poetry install --no-interaction --no-ansi --no-root --only main

CMD [ "universql.protocol.snowflake.handler" ]

