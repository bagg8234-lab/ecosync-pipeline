FROM python:3.12-alpine

WORKDIR /app

RUN apk add --no-cache gcc musl-dev libpq-dev

COPY requirements.txt .
run pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

CMD ["tail", "-f", "/dev/null"]