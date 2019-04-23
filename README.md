# pubsub-push-proxy

Pull messages from [GCP PubSub](https://cloud.google.com/pubsub/) and POST to an HTTP endpoint.

## Status

Under development.  Works in basic testing.

## Motivation

If you are using GCP PubSub and wish to use push but cannot, then this may help.

Reasons you may need to do this:

- You may have an application that you are unable to use [PubSub push authentication](https://cloud.google.com/pubsub/docs/push#authentication_and_authorization).
- You cannot expose your application so that PubSub can directly access it - ie, you are on a private network.

You may also have a code base that you are unable or unwilling to add PubSub functionality.  `pubsub-push-proxy` may allow your application to handle pubsub messages by handling a normal HTTP POST.

## Usage

`pubsub-push-proxy` uses [Google default application credentials](https://cloud.google.com/docs/authentication/production#finding_credentials_automatically). This generally means you need to create a [service account and key](https://cloud.google.com/iam/docs/service-accounts).

Clone this repository and build the binary.  Tested with Go 1.12.x:

```shell
mkdir -p $HOME/src
cd $HOME/src
git clone https://github.com/bakins/pubsub-push-proxy.git
cd pubsub-push-proxy
go build .
```

You should now have a `pubsub-push-proxy` binary.  

```shell
./pubsub-push-proxy --help

pubsub-push-proxy: main --project=PROJECT --endpoint=ENDPOINT --subscription=SUBSCRIPTION [<flags>]

Flags:
  -h, --help                   Show context-sensitive help (also try --help-long and --help-man).
  -p, --project=PROJECT        GCP project
  -l, --log-level=LOGLEVEL     log level: valid options are debug, info, warn, and error
  -e, --endpoint=ENDPOINT      URL to POST pubsub message
  -m, --max-extension=5m       maximum period for which the Subscription should automatically extend the ack deadline for each message
  -o, --max-outstanding-messages=8
                               maximum number of unprocessed messages (unacknowledged but not yet expired)
  -s, --subscription=SUBSCRIPTION
                               name of the PubSub subscription
  -r, --retries=10             number of times to retry an HTTP POST
  -a, --addr="127.0.0.1:8080"  listen address for metrics handler
```

`pubsub-push-proxy` will pull messages from the PubSub subscription and perform HTTP POSTs to the given endpoint.  The body of the post request is JSON described here https://cloud.google.com/pubsub/docs/push#receiving_push_messages

Only HTTP status code 200 is considered successful.

## License

See [LICENSE](./LICENSE)


