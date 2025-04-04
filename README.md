# Kafka-Tester
Kafka Tester

## Getting kafkauser info ready for go app
*NOTE if you are in java you need to build a trust store instead, the secret will default to a .p12 format


For a Go app we will just need the following:

- user.crt
- user.key
- ca.crt

I created a user called `test-kafka-user` and the broker is in `kafka-tutorial-kraft-east`

```bash
oc get secret test-kafka-user -n kafka-tutorial-kraft-east -o jsonpath='{.data.ca\.crt}' | base64 -d > ca.crt
oc get secret test-kafka-user -n kafka-tutorial-kraft-east -o jsonpath='{.data.user\.crt}' | base64 -d > user.crt
oc get secret test-kafka-user -n kafka-tutorial-kraft-east -o jsonpath='{.data.user\.key}' | base64 -d > user.key
```

we should be able to test the connectivity before running the app

```bash

openssl s_client -connect my-cluster-kraft-kafka-bootstrap-kafka-tutorial-kraft-east.apps.axolab.axodevelopment.dev:443 -cert user.crt -key user.key -CAfile ca.crt -verify_return_error
```