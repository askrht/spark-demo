A mobile friendly presentation can be viewed [here](https://askrht.github.io/spark-demo/) It has been generated from the Rmarkdown present inside `docs`

**Prerequisite**

  1. Tested on `Ubuntu 16.04`
  1. Requires `docker 18.09.6` and `docker-compose 1.24.0` to be available and ready to use
  1. Requires `make`
  1. Execute the commands in bash
  1. Assumes that input data is clean

  You may need to configure a proxy to pull the docker images. <strong>Do not run in production !!</strong>

**Start here**

  Data pipeline executes inside the docker containers, on a development machine. The entire pipeline is automated through a self documented makefile. Executing `make` command in the root of this repository will build the docker containers for spark and hadoop, start them, verify input data and generate the report

  Either execute `make` in the root of the repository or execute individual commands `make setup start verify report`. Most commands are idempotent

   Explore other commands using `make help`

  ```
  $ make help
  all                            setup start verify report
  clean-output                   Delete output data
  connect                        To enter the Spark container
  report                         Print the output report and save it to a file
  setup                          Build Docker containers
  start                          Starts Spark and Hadoop. Jupyter is at localhost:8888
  stop                           Stop and remove the containers
  verify                         Check if the input data is skewed
  ```

  Output of `make report` is shown below. It is saved locally as well as in Hadoop

  ```
  $ make report
  AK#2016#8#1#11#123458
  AK#2016#8#1##123458
  AK#2016#8###123458
  AK#2016####123458
  AK#####123458
  AL#2017#8#1#10#123457
  AL#2017#8#1##123457
  AL#2017#8###123457
  AL#2017####123457
  AL#2016#8#1#12#123459
  AL#2016#8#1##123459
  AL#2016#8###123459
  AL#2016####123459
  AL#####246916
  CA#2016#2#1#9#246912
  CA#2016#2#1##246912
  CA#2016#2###246912
  CA#2016####246912
  CA#####246912
  OR#2016#2#1#9#123456
  OR#2016#2#1##123456
  OR#2016#2###123456
  OR#2016####123456
  OR#####123456
  The report has been saved to hdfs://hadoop:9000/output and locally at dataout/sales_by-state.txt
  ```

**When finished**

  Execute `make stop` on your host machine. This stops and removes the containers
