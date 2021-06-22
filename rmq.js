let amqp = require(`amqplib/callback_api`);

const RMQ_CONNSTRING = process.env.RMQ_CONNSTRING || null;

if (!RMQ_CONNSTRING) {
  console.log(`No Rabbit-MQ Connection string`);
  process.exit(0);
}

class RabbitMQ {
  static connect() {
    return new Promise((resolve, reject) => {
      amqp.connect(RMQ_CONNSTRING, (err, conn) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(conn);
      });
    });
  }

  static initExchangeChannel(connection, exchangeName, exchangeType = 'topic') {
    return new Promise((resolve, reject) => {
      connection.createChannel((err, chan) => {
        if (err) {
          reject(err);
          return;
        }
        chan.assertExchange(exchangeName, exchangeType);
        resolve(chan);
      });
    });
  }

  static initQueue(
    channel,
    exchangeName,
    queueName,
    subscribedTopics = [],
    options = {}
  ) {
    return new Promise((resolve, reject) => {
      channel.assertQueue(queueName, {}, (err, q) => {
        if (err) {
          reject(err);
          return;
        }
        if (options.prefetch && options.prefetchCount)
          channel.prefetch(options.prefetchCount);

        if (subscribedTopics.length < 1)
          channel.bindQueue(q.queue, exchangeName, '');
        else {
          subscribedTopics.forEach((x) =>
            channel.bindQueue(q.queue, exchangeName, x)
          );
        }
        resolve(q);
      });
    });
  }
}

module.exports = RabbitMQ;
