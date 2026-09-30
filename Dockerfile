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
COPY ignition1093_app.py .
COPY ignition110_app.py .
COPY orderbook_patch.py .
COPY warmup_patch.py .
COPY ignition110_entry.py .
COPY ignition114_entry.py .
COPY ignition115_entry.py .
COPY ignition1151_entry.py .
COPY ignition116_entry.py .
COPY ignition117_entry.py .
COPY ignition118_entry.py .
COPY ignition1181_entry.py .

ENV PORT=8080
ENV PSI_SCANNER_VERSION=10.18.1

CMD ["python", "ignition1181_entry.py"]
