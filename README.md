# notification-service-local

## Steps to run locally
1. Make sure [Docker](https://docs.docker.com/get-docker/) and [k6](https://k6.io/docs/get-started/installation/) is installed.
2. Compose the project using the command -  
```bash
    docker compose build
```
3. Run the project in detached mode using the command -  
```bash
docker compose up -d
```
4. Run the test script inside the folder `test-scripts` using the command -   
```bash
k6 run loadtest.js
```

## How k6 works
k6 is a load testing tool that can be used to test the performance of a system under load by simulating a large number of users accessing the system at the same time.

The `export default function()` defines the main function that is executed for each iteration of the test. It contains the url to the notification service and the payload to be sent to it. Each virtual user (VU) executes this function for a duration of time defined in the `options` object at the start of the script. The `stages` defines the different stages of the test. At each stage, we can define the number of virtual users and the duration of the stage. For example, if the 1st stage is `{ duration: '30s', target: 1 }`, it means that for 30 seconds, 1 virtual user will be executing the `export default function()`. If the 2nd stage is `{ duration: '30s', target: 10 }`, it means that for the next 30 seconds, the number of VUs will go from 1 to 10. 

## Measurement Tools
Along with k6, which shows the client side metrics, we have integrated Prometheus and Jaeger to show the server side metrics and traces respectively. We have also integrated Grafana to visualize the metrics recorded by Prometheus.

### How to access the measurement tools
1. Prometheus: http://localhost:9090
2. Jaeger: http://localhost:16686
3. Grafana: http://localhost:3000
4. Mailhog: http://localhost:8025

## Testing Flow
1. Run the docker containers with the command specified above.
    - If you want to run multiple containers for each service, you can use the `--scale` flag for each container. For example, to run 2 containers for each service, you can use the command -  
    ```bash
    docker compose up -d --scale notification=2 --scale user-preference=2 --scale scheduler=2 --scale consumer=2
    ```

2. Run the test script with the desired stages.

3. After the test is complete, you can view the client side metrics in the terminal and server side metrics in the Grafana dashboard. In Grafana, login with the credentials `admin` and `admin`. Then click on Dashboards and view the desired dashboard.

4. You can view the traces in Jaeger. Select the Service for which you want to view the traces and the Operation (for ex. send_email_endpoint for notification-service) for that service. Then click on Find Traces. If selected notification service, it will show the entire flow from the client, to the user-preference service, to the consumer service.

## How to shutdown the containers
1. To shutdown the containers, use the command -
```bash
    docker compose down
```
2. To shutdown the containers and remove the volumes, use the command -
```bash
    docker compose down --volumes --remove-orphans
```