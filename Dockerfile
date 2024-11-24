FROM python:3.11-slim-bullseye

RUN apt-get update && apt-get -y upgrade \
    && apt-get install gcc -y  \
    && pip install --upgrade pip

# Set separate working directory for easier debugging.
WORKDIR /app

RUN pip install 'poetry==1.8.3'
COPY pyproject.toml poetry.lock ./

RUN poetry config virtualenvs.create false --local

# Copy everything. (Note: If needed, we can use .dockerignore to limit what's copied.)
COPY . .

RUN poetry install

EXPOSE 8084
ENV SERVER_HOST=0.0.0.0
ENV USE_LOCALCOMPUTING_COM=1

ENTRYPOINT ["poetry", "run", "universql"]