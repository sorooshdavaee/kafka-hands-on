# Topics / consumer groups / bootstrap defaults for the hands-on lab.
param(
  [string]$Bootstrap = "localhost:29092,localhost:39092,localhost:49092"
)

$ErrorActionPreference = "Stop"
$kafkaBinHint = "Run via docker: docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh ..."

Write-Host "Describe orders topic (Leader / Replicas / ISR):"
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka-1:19092 `
  --describe --topic orders

Write-Host "`nConsumer lag for payment-group:"
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server kafka-1:19092 `
  --describe --group payment-group
