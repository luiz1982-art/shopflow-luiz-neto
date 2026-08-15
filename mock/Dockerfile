FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir pika requests

COPY mock_antifraude.py .

CMD ["python", "mock_antifraude.py"]