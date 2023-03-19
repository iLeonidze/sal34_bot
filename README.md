# sal34_bot


### Running using Docker Compose

1. Prepare configuration in `./configs/`

2. Start using docker-compose
   ```bash
   docker-compose up -d
   ```


### Running using Python

1. Install requirements:
   ```bash
   pip3 install -r requirements.txt
   ```

2. Prepare configuration in `./configs/`

3. Run application:
   ```bash
   python3 main.py
   ```


### Running using Docker

1. Build Docker image, it can take some time
   ```bash
   docker build -t sal34_bot --no-cache .
   ```

2. Prepare configuration in `./configs/`

3. Start container
   ```bash
   docker run -d -it --name sal34_bot sal34_bot \
     --mount type=bind,source="$(pwd)"/buildings,target=/opt/bot/buildings \
     --mount type=bind,source="$(pwd)"/configs,target=/opt/bot/configs \
     --mount type=bind,source="$(pwd)"/data,target=/opt/bot/data \
     --mount type=bind,source="$(pwd)"/stats,target=/opt/bot/stats \
     --mount type=bind,source="$(pwd)"/users,target=/opt/bot/users
   ```
