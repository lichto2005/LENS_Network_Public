# LENS Networking

This repository holds all of the backend networking required to communicate
LENS with the cloud.

Communications happen roughly in two parts, local between model and backend,
and remote between backend and cloud.

## Local

On the local side, we handle messaging using a TCP socket server/client
style. We accept artibrary connections for debugging, and respond to all
messages, effectively giving us a backdoor for testing. To connect, we can
for example: `nc localhost 48879` and then shoot messages at will.

We use this pipe for the model to send us PredictionMessages, primarily.

## Cloud

On the cloud side, we have a wrapper around the AWS client, IOTWrapper.
This maintains a global IOT Client that we can pub/sub to.

Connection is more complicated here, and we need three things: a root
certificate authority, a public key, and a device certificate. These
have standardized names and will default load from the `./` directory,
although can also be loaded through the environment variables `IOT_CA`,
`IOT_KEY`, and `IOT_CERT`.

# Running

## Setup

```
pip install -r requirements.txt
```

## Execution

For testing, run `python -i MessageHandler.py` from the project root directory
after placing your IOT certs in that directory. This will spawn 2 initial
threads to handle local comms and cloud comms respectively.

