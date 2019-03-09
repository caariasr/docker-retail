FROM python:3.6
WORKDIR /docker-exito
VOLUME /docker-exito
COPY . /docker-exito
RUN pip install -r requirements.txt

CMD [ "python", "./service.py" ]
