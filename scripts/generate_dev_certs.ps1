param(
  [string]$OutDir = ".dtruntime-certs"
)

$ErrorActionPreference = "Stop"
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$caKey = Join-Path $OutDir "ca.key"
$caCert = Join-Path $OutDir "ca.crt"
$serverKey = Join-Path $OutDir "server.key"
$serverCsr = Join-Path $OutDir "server.csr"
$serverCert = Join-Path $OutDir "server.crt"
$clientKey = Join-Path $OutDir "client.key"
$clientCsr = Join-Path $OutDir "client.csr"
$clientCert = Join-Path $OutDir "client.crt"

openssl genrsa -out $caKey 2048
openssl req -x509 -new -nodes -key $caKey -sha256 -days 365 -subj "/CN=dtruntime-dev-ca" -out $caCert
openssl genrsa -out $serverKey 2048
openssl req -new -key $serverKey -subj "/CN=localhost" -out $serverCsr
openssl x509 -req -in $serverCsr -CA $caCert -CAkey $caKey -CAcreateserial -out $serverCert -days 365 -sha256
openssl genrsa -out $clientKey 2048
openssl req -new -key $clientKey -subj "/CN=dtruntime-client" -out $clientCsr
openssl x509 -req -in $clientCsr -CA $caCert -CAkey $caKey -CAcreateserial -out $clientCert -days 365 -sha256

Remove-Item $serverCsr,$clientCsr -ErrorAction SilentlyContinue
Write-Host "Generated dev certs in $OutDir"
