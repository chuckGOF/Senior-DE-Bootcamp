FROM python:3.11-slim

# Avoid Python writing pyc files
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Set Python path
ENV PYTHONPATH=/app

# Expose port
EXPOSE 8000

CMD ["python", "week1_basics/src/main.py"]