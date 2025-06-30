FROM python:3.10-slim

# Atualiza pacotes e instala dependências básicas do sistema
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    unixodbc \
    unixodbc-dev \
    odbcinst \
    curl

# Instalar o driver ODBC da Microsoft (exemplo para SQL Server)
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
 && curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Instalar dependências para o pyodbc
RUN apt-get install -y build-essential

# Depois instalar o pyodbc via pip (ou requirements.txt)
RUN pip install pyodbc

# Define diretório de trabalho
WORKDIR /app

# Copia arquivos para dentro do container
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Expõe a porta do app (por exemplo, 8501 para Streamlit)
EXPOSE 8501

# Comando para rodar o app
CMD ["streamlit", "run", "app2.py", "--server.port=8501", "--server.address=0.0.0.0"]
