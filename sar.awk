# gawk -v HOST=hostname -f sar.awk hostname.sar
function isoDate(d1) {
  iso = d1;
  if(match(d1, /([0-9]{2,2})\/([0-9]{2,2})\/([0-9]{4,4})/, m)){ # 5/24/2019
    iso = m[3] "-" m[1] "-" m[2]
  }
  else if(match(d1, /([0-9]{2,2})\/([0-9]{2,2})\/([0-9]{2,2})/, m)) { # 5/24/18
    iso = "20" m[3] "-" m[1] "-" m[2]
  }
  return iso;
}
function to24hr(t1, ampm) {
  split(t1, t2, ":");
  hr = t2[1]; mm = t2[2]; ss = t2[3];
  if(ampm == "PM") hr = hr + 12;
  t3 = hr ":" mm ":" ss;
  return t3;
}
BEGIN {
  section = 0;
  sections="CPU proc/s pgpgin/s tps kbmemfree runq-sz IFACE DEV active/s";
  n=split(sections, s, " ");
  for(i=1; i<=n; i++) {
    metric = s[i];
    sub("/", "", metric);
    metric_name[s[i]] = metric;
  }
}
{
  if(match($0, /^Linux .*/)) {
    # Linux 4.15.0-51-generic (jupyter) 	07/14/2019 	_x86_64_	(4 CPU)
    dt = isoDate($4);
    section++;
  }
  if(metric_name[$3] != "") {
    # 11:00:04 AM       CPU     %user    %nice   %system   %iowait    %steal     %idle
    # 11:00:04 AM    proc/s   cswch/s
    # 11:00:04 AM  pgpgin/s pgpgout/s   fault/s  majflt/s  pgfree/s pgscank/s pgscand/s pgsteal/s    %vmeff
    # 11:00:04 AM       tps      rtps      wtps   bread/s   bwrtn/s
    # 11:00:04 AM kbmemfree kbmemused  %memused kbbuffers  kbcached  kbcommit   %commit  kbactive   kbinact   kbdirty
    # 11:00:04 AM   runq-sz  plist-sz   ldavg-1   ldavg-5  ldavg-15   blocked
    # 11:00:04 AM     IFACE   rxpck/s   txpck/s    rxkB/s    txkB/s   rxcmp/s   txcmp/s  rxmcst/s   %ifutil
    # 11:00:04 AM     IFACE   rxerr/s   txerr/s    coll/s  rxdrop/s  txdrop/s  txcarr/s  rxfram/s  rxfifo/s  txfifo/s
    output_file=HOST "_" metric_name[$3] ".dat";
    if(output_file == "IFACE.dat" && ($NF == "txfifo/s" || NF >= 12)) output_file = "EDEV.dat"
    if(section == 1) {
      $1=""; $2="";
      print("date", "time", $0) > output_file;
    }
  }
  else if(match($0, /^[0-9][0-9]:[0-9][0-9]:[0-9][0-9]/)) {
    # 11:00:04 AM     all      9.56      0.00      1.25      0.02      0.00     77.65
    t1=to24hr($1, $2); $1=""; $2="";
    print(dt, t1, $0) >> output_file;
  }
}
