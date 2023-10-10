# Source Code

Most of the code lives here. Each of the different job steps are implemented as separate pipeline runs (in the future, once this lives on AWS, this will likely be either implemented via step functions or Airflow). These pipelines are in the `pipelines` directory, and they contain the workflows for syncing data, performing transformations, and messaging Reddit users. Each of these pipelines consists of separate microservices (implemented as lambdas), each of which lives in the `services` directory.

General helper functionalities live in `lib`, data dumps live in `data` (and, once hooked up to AWS, will live in S3), and the old legacy pipeline lives in `legacy`.
