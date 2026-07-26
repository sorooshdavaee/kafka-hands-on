# Topics / consumer groups / bootstrap defaults for the hands-on lab.
param(
  [string]$Bootstrap = "localhost:9092,localhost:9093,localhost:9094"
)

$ErrorActionPreference = "Stop"
$kafkaBinHint = "Run via docker: docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh ..."

Write-Host "Describe orders topic (Leader / Replicas / ISR):"
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka-1:29092 `
  --describe --topic orders

Write-Host "`nConsumer lag for payment-group:"
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server kafka-1:29092 `
  --describe --group payment-group
