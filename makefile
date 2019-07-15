.PHONY: help
.DEFAULT_GOAL := all
help: # https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
setup: ## Build Docker containers
	docker-compose build
start: stop ## Starts Spark and Hadoop. Jupyter is at localhost:8888
	docker-compose up -d
	docker exec jupyter /usr/local/spark/sbin/start-history-server.sh > /dev/null 2>&1
	echo "Wait for hadoop to start"
	sleep 60
	-docker exec hadoop /work/hadoop/setenv.sh > /dev/null 2>&1
	sleep 10
connect: ## To enter the Spark container
	docker exec -it --user jovyan jupyter bash
stop: ## Stop and remove the containers
	docker-compose down
verify: clean-output ## Check if the input data is skewed
	echo "Verifying input data"
	docker exec --user jovyan jupyter bash -c 'source work/jupyter/setenv.sh && spark-submit check_input.py 2>/dev/null'
	echo "Finished verifying input data"
report: clean-output ## Print the output report and save it to a file
	test "`basename ${PWD}`" = "spark-demo" || die "Expecting to be inside phweb instead of `basename ${PWD}`\n"
	docker exec --user jovyan jupyter bash -c 'source work/jupyter/setenv.sh && spark-submit sales_by_state.py 2> /dev/null'
	# sed 's/ 0/#/g' | sed 's/ /#/g'
	echo "The report has been saved to hdfs://hadoop:9000/dataout/output and locally at dataout/sales_by_state.txt"
clean-output: ## Delete output data
	mkdir -p dataout/spark-log && chmod 777 dataout && chmod 777 dataout/spark-log
	rm -rf dataout/spark-log/*
	rm -rf dataout/*.txt
	-docker exec hadoop bash -c '/usr/local/hadoop/bin/hadoop fs -rm -r /dataout/output > /dev/null 2>&1'
	# wait for hadoop to finish the job
	sleep 10
all: setup start verify report
