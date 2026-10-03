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
COPY ignition1182_entry.py .
COPY ignition1183_entry.py .
COPY ignition1184_entry.py .
COPY ignition1185_entry.py .
COPY ignition119_entry.py .
COPY ignition1191_entry.py .
COPY ignition1192_entry.py .
COPY ignition120_entry.py .
COPY ignition121_entry.py .
COPY ignition1211_entry.py .
COPY ignition1212_entry.py .
COPY ignition1213_entry.py .
COPY ignition1214_entry.py .
COPY psi_v11_entry.py .
COPY psi_v11_1_entry.py .
COPY psi_v11_2_entry.py .
COPY psi_v11_2_1_entry.py .
COPY psi_v11_2_2_entry.py .
COPY psi_v11_3_entry.py .
COPY psi_v11_3_1_entry.py .
COPY psi_v11_3_2_entry.py .
COPY psi_v11_3_3_entry.py .
COPY psi_v11_3_4_entry.py .
COPY psi_v11_3_5_entry.py .
COPY psi_v11_3_6_entry.py .
COPY psi_v11_3_7_entry.py .
COPY psi_v11_4_entry.py .
COPY psi_v11_5_entry.py .

ENV PORT=8080
ENV PSI_SCANNER_VERSION=11.0.5.16
ENV PSI_V11_SHADOW_ONLY=1
ENV PSI_BYBIT_ENABLED=0

CMD ["python", "psi_v11_5_entry.py"]