FROM python:3.11-slim


ENV APP_HOME /app 


WORKDIR $APP_HOME


COPY . .


RUN pip install --no-cache -r requirements.txt 


VOLUME $APP_HOME/db



ENTRYPOINT [ "python", "hw_06/main.py" ]

