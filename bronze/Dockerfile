# Base image
FROM python:3.12-slim

# Evita geração de .pyc
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Define diretório dentro do container
WORKDIR /app

# Copia apenas o src
COPY src/ ./src/

# Copia requirements
COPY requirements.txt .

# Instala dependências
RUN pip install --no-cache-dir -r requirements.txt

# Adiciona src ao PYTHONPATH
ENV PYTHONPATH=/app/src

# Comando para executar
CMD ["python", "src/main.py"]