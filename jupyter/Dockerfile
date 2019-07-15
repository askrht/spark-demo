FROM jupyter/all-spark-notebook:6c3390a9292e
USER root
ADD spark-defaults.conf /usr/local/spark/conf/
RUN apt-get update && \
    apt-get install -y --no-install-recommends vim sysstat gawk && \
    rm -rf /var/lib/apt/lists/*
USER ${NB_USER}
RUN echo "source /home/${NB_USER}/work/jupyter/setenv.sh" >> /home/${NB_USER}/.bashrc
CMD ["start-notebook.sh", "--LabApp.token=''"]
