FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
# La DB doit être générée AVANT le build
COPY data/finess_occitanie.db data/finess_occitanie.db
CMD ["uvicorn", "api.api:app", "--host", "0.0.0.0", "--port", "8080"]