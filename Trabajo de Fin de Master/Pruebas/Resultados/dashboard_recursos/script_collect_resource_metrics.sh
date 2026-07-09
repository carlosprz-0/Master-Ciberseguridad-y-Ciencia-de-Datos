#!/bin/bash

PLATFORM=$1

if [ -z "$PLATFORM" ]; then
  echo "Uso: ./collect_resource_metrics.sh pc_personal"
  echo "o:   ./collect_resource_metrics.sh raspberry_pi4"
  exit 1
fi

if [ ! -x /usr/bin/time ]; then
  echo "ERROR: /usr/bin/time no está instalado."
  echo "Instálalo con: apt update && apt install -y time"
  exit 1
fi

OUT="resource_metrics_${PLATFORM}.csv"
TMP="tmp_time_output.txt"

echo "platform,cipher,level,user_time_s,system_time_s,cpu_percent,elapsed_time,max_rss_kb" > "$OUT"

for cipher in ascon-aead128 aes-ccm128; do
  for level in 512 768 1024; do

    echo "Ejecutando $cipher nivel $level en $PLATFORM..."

    /usr/bin/time -v ./client_main --attack -s "$level" -c "$cipher" --iterations 1000 > /dev/null 2> "$TMP"

    user_time=$(grep "User time (seconds)" "$TMP" | awk -F': ' '{print $2}')
    system_time=$(grep "System time (seconds)" "$TMP" | awk -F': ' '{print $2}')
    cpu_percent=$(grep "Percent of CPU this job got" "$TMP" | awk -F': ' '{print $2}' | tr -d '%')
    elapsed_time=$(grep "Elapsed (wall clock) time" "$TMP" | awk -F': ' '{print $2}')
    max_rss=$(grep "Maximum resident set size" "$TMP" | awk -F': ' '{print $2}')

    echo "$PLATFORM,$cipher,$level,$user_time,$system_time,$cpu_percent,$elapsed_time,$max_rss" >> "$OUT"

  done
done

rm -f "$TMP"

echo "Resultados guardados en $OUT"
