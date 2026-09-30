FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY qualifier_app.py .
COPY target10_app.py .
COPY stable10_app.py .
COPY ignition10_app.py .
COPY ignition1071_app.py .
COPY ignition108_app.py .
COPY ignition1081_app.py .
COPY ignition109_app.py .
COPY ignition1091_app.py .
COPY ignition1092_app.py .

ENV PORT=8080

CMD ["python", "ignition1092_app.py"]
