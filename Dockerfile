FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY qualifier_app.py .
COPY target10_app.py .
COPY stable10_app.py .

ENV PORT=8080

CMD ["python", "stable10_app.py"]
