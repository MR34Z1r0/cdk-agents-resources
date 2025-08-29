FROM public.ecr.aws/lambda/python:3.11

# Instalar herramientas y librerías necesarias para compilar Pillow y lxml
RUN yum install -y \
    gcc \
    libjpeg-turbo-devel \
    zlib-devel \
    freetype-devel \
    libpng-devel \
    libxml2-devel \
    libxslt-devel \
    && yum clean all

# Copiar requirements.txt e instalar dependencias
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copiar el archivo .whl y lo instalamos
COPY aje_libs-0.1.0-py3-none-any.whl .
RUN pip install aje_libs-0.1.0-py3-none-any.whl

# Copiar el código de la función Lambda
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Comando que se ejecutará cuando se invoque la función
CMD [ "lambda_function.lambda_handler" ]