FROM public.ecr.aws/lambda/python:3.11

RUN pip install 'poetry==1.8.3'
# Copy everything. (Note: If needed, we can use .dockerignore to limit what's copied.)
COPY . ${LAMBDA_TASK_ROOT}
RUN poetry config virtualenvs.create false --local && poetry install --no-interaction --no-ansi --no-root --only main

CMD [ "universql.protocol.lambda.snowflake" ]


