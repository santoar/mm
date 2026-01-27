# Python image use karein
FROM python:3.10-slim

# App directory set karein
WORKDIR /app

# Dependencies copy aur install karein
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pura code copy karein
COPY . .

# Cloud Run $PORT environment variable use karta hai
ENV PORT 8080

# App start karein (Gunicorn use karna best hai production ke liye)
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app