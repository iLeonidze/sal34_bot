FROM python:slim

RUN mkdir /opt/bot \
 && groupadd --gid 1000 bot \
 && useradd --uid 1000 --gid 1000 -m bot \
 && chmod 700 -R /opt/bot \
 && chown bot:bot -R /opt/bot

COPY assistant.py /opt/bot/assistant.py
COPY main.py /opt/bot/main.py
COPY requirements.txt /opt/bot/requirements.txt

WORKDIR /opt/bot

USER bot

RUN pip install --no-cache-dir -r requirements.txt \
 && rm -f requirements.txt

ENTRYPOINT ["python3", "main.py"]
