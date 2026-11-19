# librdkafka clients: verify the broker by its root CA thumbprint (no CA file)

Example: [root_ca_thumbprint.c](root_ca_thumbprint.c) — a C producer using
[librdkafka](https://github.com/confluentinc/librdkafka) **as is** (no librdkafka changes).

Normally a librdkafka client verifies the broker's TLS certificate against CA certificates it has locally
(`ssl.ca.location`, `ssl.ca.pem` or the OS trust store). Distributing and updating those CA files on every client
machine is a chore — and a risk when a shared OS store trusts far more CAs than your Kafka cluster needs.

Simpler: let the broker send its **full certificate chain** and give the client only the **SHA-256 thumbprint of the
root CA**. librdkafka's existing `ssl.certificate.verify_cb` does the rest.

## How it works

The broker sends `leaf → intermediate CA → root CA`. OpenSSL calls the verify callback once per certificate,
**root first** (`depth` 2), then the intermediate (`depth` 1), then the leaf (`depth` 0):

| Certificate | What OpenSSL reports (`*x509_error`) | What the callback does |
|---|---|---|
| Root CA | `X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN` — self-signed, not in a trust store | Accept **only if SHA-256(DER) equals a pinned thumbprint**: set `*x509_error = 0`, return 1 |
| Intermediate CA | `0` if its signature verifies **with the root's public key**, dates and CA flag are valid | Accept only if `*x509_error == 0` |
| Leaf (broker) | `0` if its signature verifies with the intermediate's key, dates valid, hostname matches | Accept only if `*x509_error == 0` and the root of *this* handshake matched |

Once the callback accepts the pinned root, OpenSSL continues its normal checks down the chain — the root validates the
intermediate, the intermediate validates the leaf. The application only pins the root.

The callback's signature (`rd_kafka_conf_set_ssl_cert_verify_cb`):

```c
int verify_cb(rd_kafka_t *rk, const char *broker_name, int32_t broker_id,
              int *x509_error,           /* in: OpenSSL's result, out: override */
              int depth,                 /* 0 = leaf */
              const char *buf, size_t size, /* the certificate, DER */
              char *errstr, size_t errstr_size, void *opaque);
/* return 1 = accept, 0 = reject (the handshake fails) */
```

Details that matter:

- **Require the pin at the top of every chain**, even when OpenSSL already trusts it (error `0`, e.g. a CA in a system
  store) — otherwise a CA that happens to be installed bypasses the pin.
- **Don't override other errors**: an expired or otherwise invalid root, a bad signature, an expired leaf, a hostname
  mismatch must stay fatal.
- The callback runs on librdkafka's broker threads; the example keeps the per-chain state in thread-local variables.
- `depth` is OpenSSL's depth (0 = leaf), although the callback is called root first. OpenSSL may call it twice for the
  root (once with the error, once after the chain checks) — harmless.
- The thumbprint is public: it needs **integrity** (deliver it like other trusted configuration), not secrecy. Pin
  several thumbprints (comma-separated) to rotate a root CA without downtime.

## The broker side

The broker must send the whole chain, root included.

- **This fork's OS certificate store** (`OsCertStoreSslEngineFactory`, see the top-level README) enforces it:
  `chain.pem` must be leaf → intermediate(s) → root, and the broker logs each certificate's SHA-256 thumbprint at
  startup — the root's line is the value to pin:
  `Own certificate chain sent to peers: [2] root O=…,CN=… sha256=…`
- **Standard Kafka keystores** work too: put the full chain in the keystore entry (leaf first, then intermediate(s),
  then root), e.g. `openssl pkcs12 -export -in leaf.crt -certfile intermediate-and-root.crt -inkey leaf.key`, or a PEM
  keystore (`ssl.keystore.type=PEM`, `ssl.keystore.certificate.chain`).

Check what a broker sends, and get the thumbprint from the root CA certificate:

```sh
openssl s_client -connect broker1:9093 -showcerts </dev/null | grep -E '^ [0-9] s:'
#  0 s:/CN=broker1/O=Example
#  1 s:/CN=Example Intermediate CA/O=Example
#  2 s:/CN=Example Root CA/O=Example
openssl x509 -in root-ca.crt -noout -fingerprint -sha256
```

## Build and run

```sh
# against an installed librdkafka (package or `make install`), which brings OpenSSL
cc root_ca_thumbprint.c -o root_ca_thumbprint $(pkg-config --cflags --libs rdkafka libcrypto)

./root_ca_thumbprint -b broker1:9093,broker2:9093 -t test \
  -s 15476b7b3769dae522a338cbe6f2f3c5b202d75ff866830e96801c326a8823bf \
  -X security.protocol=ssl \
  -X ssl.certificate.location=client.crt -X ssl.key.location=client.key
```

No `ssl.ca.location` is set. For SASL_SSL, set `security.protocol=sasl_ssl` and your SASL settings; the TLS part is
the same. Any other librdkafka property can be passed with `-X name=value`.

## Tested

librdkafka v2.16.0 with OpenSSL 3.5.8, against a 2-broker Kafka 3.9 cluster of this fork (OS certificate store,
brokers send `broker → Kafka Intermediate CA → Local Kafka CA`) and against test TLS servers presenting forged chains:

| Case | Result |
|---|---|
| Correct thumbprint (mutual TLS) | connected, message delivered |
| Wrong thumbprint (another real root CA) | handshake refused: "not a pinned root CA" |
| Thumbprint list `wrong,correct` (root rotation) | connected |
| Fake root with the **same name**, different key | refused at the root: thumbprint differs |
| **Real root**, but the intermediate is signed by another key | root accepted, then refused at depth 1: "certificate signature failure" |
| Leaf only (no intermediate, no root) | refused: top of the chain is not a pinned root |
| Real chain, hostname not in the certificate | refused at depth 0: "hostname mismatch" |
| Invalid thumbprint string | rejected at startup |
