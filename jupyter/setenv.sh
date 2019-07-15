#!/bin/bash
export PATH=${PATH}:/usr/local/spark/bin
collect-sar() {
  test ${#} -lt 1 && \
  echo "${FUNCNAME[0]} collects sar stats every second for the duration in seconds" && \
  echo "${FUNCNAME[0]} seconds_to_run" && \
  echo "${FUNCNAME[0]} 500" && \
  return 1
  test ${EUID} -ne 0 && \
  echo "${FUNCNAME[0]} must be run as root" && \
  return 1
  seconds_to_run=${1}
  echo "Running ${FUNCNAME[0]}"
  sudo rm -f /var/log/sysstat/sa* && \
  sudo echo 'ENABLED="true"' > /etc/default/sysstat && \
  sudo service sysstat restart && \
  for i in `seq 1 ${seconds_to_run}` ; do /usr/lib/sysstat/debian-sa1 1 1 ; sleep 1 ; done
  sudo echo 'ENABLED="false"' > /etc/default/sysstat && \
  sudo service sysstat restart
  echo "Finished ${FUNCNAME[0]}"
}
parse-sar() {
  test ${EUID} -ne 0 && \
  echo "${FUNCNAME[0]} must be run as root" && \
  return 1
  pushd . > /dev/null 2>&1 && \
  cd /var/log/sysstat && \
  sar_files_pattern="./sa[0-9][0-9]" && \
  for file_name in ${sar_files_pattern}; do
   test "${file_name}" = "${sar_files_pattern}" && \
    printf "%s\n" "No files found matching ${sar_files_pattern}" && break
    LC_TIME=POSIXCT sar -i 1 -udbBrqw -n TCP -n DEV -n EDEV -f ${file_name} > /home/jovyan/work/dataout/`hostname`.sar 2>/dev/null
  done
  cd /home/jovyan/work/dataout && \
  echo "Parsing `hostname`.sar" && \
  gawk -v HOST=`hostname` -f /home/jovyan/work/sar.awk `hostname`.sar 2>/dev/null
  chown jovyan:users * && \
  popd > /dev/null 2>&1
}
cd /home/jovyan/work
