param(
  [string]$OutDir = ".dtruntime-certs"
)

$ErrorActionPreference = "Stop"

$opensslCmd = Get-Command openssl -ErrorAction SilentlyContinue
if (-not $opensslCmd) {
  Write-Error @"
OpenSSL is required to generate development TLS/mTLS certificates.
Install OpenSSL and ensure it is on PATH.
Git for Windows commonly provides OpenSSL at:
C:\Program Files\Git\usr\bin\openssl.exe
"@
  exit 1
}

$openssl = $opensslCmd.Source

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$caKey = Join-Path $OutDir "ca.key"
$caCert = Join-Path $OutDir "ca.crt"
$serverKey = Join-Path $OutDir "server.key"
$serverCsr = Join-Path $OutDir "server.csr"
$serverExt = Join-Path $OutDir "server.ext"
$serverCert = Join-Path $OutDir "server.crt"
$clientKey = Join-Path $OutDir "client.key"
$clientCsr = Join-Path $OutDir "client.csr"
$clientExt = Join-Path $OutDir "client.ext"
$clientCert = Join-Path $OutDir "client.crt"

@"
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=@alt_names

[alt_names]
DNS.1=localhost
IP.1=127.0.0.1
DNS.2=runtime-coordinator
DNS.3=runtime-coordinator.dtruntime
DNS.4=runtime-coordinator.dtruntime.svc
DNS.5=runtime-coordinator.dtruntime.svc.cluster.local
"@ | Set-Content -Path $serverExt -NoNewline

@"
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
"@ | Set-Content -Path $clientExt -NoNewline

& $openssl genrsa -out $caKey 2048
& $openssl req -x509 -new -nodes -key $caKey -sha256 -days 365 -subj "/CN=dtruntime-dev-ca" -out $caCert

& $openssl genrsa -out $serverKey 2048
& $openssl req -new -key $serverKey -subj "/CN=localhost" -out $serverCsr
& $openssl x509 -req -in $serverCsr -CA $caCert -CAkey $caKey -CAcreateserial -out $serverCert -days 365 -sha256 -extfile $serverExt

& $openssl genrsa -out $clientKey 2048
& $openssl req -new -key $clientKey -subj "/CN=dtruntime-client" -out $clientCsr
& $openssl x509 -req -in $clientCsr -CA $caCert -CAkey $caKey -CAcreateserial -out $clientCert -days 365 -sha256 -extfile $clientExt

Remove-Item $serverCsr,$clientCsr,$serverExt,$clientExt -ErrorAction SilentlyContinue
Write-Host "Generated dev certs in $OutDir"
