# Sportfolios-alpha-server

This repository contains the code and configuration file to run the alpha server. The server consists of a single VM, holding four docker containers. They are 

### 1. An NGINX web server and reverse proxy

This is the entry-point for all traffic hitting the server. It is optimised for speed and reliability. It runs on ports 80 (http) an 443 (SSL) with all un-encrypted http traffic being re-routed to the SSL port. All traffic is passed to the upstream flask server. 

### 2. A Flask server, run with Gunicorn

This is where the primary server logic resides. Here is where we write all the back-end code. The process is run with Gunicorn, spawning a number of workers. There is also a sqlite database stored here, which logs all the server interactions. 

### 3. A Redis server

This is where the flask server state is stored. This means market makers can be created quickly on the fly, and all flask processes will share the same state. It runs on default port 6379. 

### 4. A certbot

This is a small process that regularly checks for SSL certificate expiry, and requests a new one via letsencrypt when necessary. 

 

