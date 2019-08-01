FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "order_simulator.py", \
     "--dsn", "postgresql://orders_user:orders_pass@postgres:5432/orders_db", \
     "--rate", "500"]
