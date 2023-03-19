# sal34_bot

### Running using Python

1. Install requirements:
   ```bash
   pip3 install -r requirements.txt
   ```

2. Prepare configuration in `configs/`

3. Run application:
   ```bash
   python3 main.py
   ```


### Running using Docker Compose

1. Prepare configuration in `configs/`

2. Build Docker image, it can take some time
   ```bash
   docker build . --no-cache
   ```

3. Start using docker-compose
   ```bash
   docker-compose up -d
   ```
