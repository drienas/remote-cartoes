if (process.env.NODE_ENV !== 'production') {
  require('dotenv').config();
}

const rmq = require('./rmq');
const axios = require('axios');
const mongoose = require(`mongoose`);

const MONGO_DB = process.env.MONGO_DB || null;
const ELASTIC_URL = process.env.ELASTIC_URL || null;

if (!MONGO_DB || !ELASTIC_URL) {
  console.error(`NOT_ALL_ENV_SET`);
  process.exit(0);
}

const ElasticIndex = `${ELASTIC_URL}/cars/_doc`;
const mongoUrl = `mongodb://${MONGO_DB}/cars`;
let iConnection, iChannel, iQueue;
let Car;

const ack = (msg) => iChannel.ack(msg, false);
const nackError = (msg) => iChannel.nack(msg, false, true);
const nack = (msg) => iChannel.nack(msg, false, false);

const carSchema = new mongoose.Schema(
  {
    fzg_id: {
      type: String,
      required: true,
      index: true,
    },
  },
  { timestamps: true, strict: false }
);

const setUpMongoDbConnection = () => {
  console.log(`Connecting to MongoDB @ ${mongoUrl}`);
  mongoose.connect(mongoUrl, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    useCreateIndex: true,
  });

  mongoose.connection.on('error', (err) => {
    console.error(err);
    process.exit(0);
  });

  mongoose.connection.on('disconnected', (msg) => {
    console.error(msg);
    process.exit(0);
  });
};

const rmqConsumeQueue = (channel, queue) => {
  console.log(`Ready to handle queued cars from rabbitmq bridge`);
  channel.consume(
    queue.queue,
    async (msg) => {
      try {
        let data;
        try {
          data = JSON.parse(msg.content.toString());
        } catch (err) {
          console.error(err);
          nack(msg);
        }
        if (!data.id) {
          console.error(`No proper FZG_ID set`);
          nack(msg);
        }
        let success = await mdbUpsertDatasetToES(data.id);
        if (success) ack(msg);
        else throw `Could not handle car`;
      } catch (err) {
        console.error(err);
        nackError(msg);
      }
    },
    { noAck: false }
  );
};

const mdbUpsertDatasetToES = (fzg_id) =>
  new Promise(async (resolve, reject) => {
    try {
      let car = await Car.findOne({ fzg_id });
      if (!car || !car._id) {
        resolve(true);
        return;
      }

      let esObjKeys = Object.keys(car._doc).filter(
        (x) => !['_id', '__v'].includes(x)
      );
      let esObj = {};
      for (let i of esObjKeys) esObj[i] = car._doc[i];

      const id = fzg_id;
      try {
        console.log(`Handling ${id}`);
        await axios.delete(`${ElasticIndex}/${id}`);
      } catch (err) {
        console.log(`Deleting existing dataset...`);
        if (
          err.response &&
          err.response.data &&
          err.response.data.status === 404
        )
          console.log(`Nothing to delete...`);
      } finally {
        console.log(`Creating new dataset...`);
        await axios.post(`${ElasticIndex}/${id}`, esObj);
        console.log(`Done.`);
        resolve(true);
      }
    } catch (err) {
      reject(err);
    }
  });

const startSync = async () => {
  console.log(`Up and ready to sync cars to mongodb`);

  iConnection = await rmq.connect();
  iChannel = await rmq.initExchangeChannel(iConnection, 'carbridge_x');

  iQueue = await rmq.initQueue(
    iChannel,
    'carbridge_x',
    'CarBc',
    ['update.r.bc'],
    {
      prefetch: true,
      prefetchCount: 1,
    }
  );

  rmqConsumeQueue(iChannel, iQueue);
};

(async () => {
  try {
    setUpMongoDbConnection();
    mongoose.connection.on('connected', (err) => {
      console.log(`Connected to MongoDB`);
      if (err) {
        console.error(err);
        process.exit(0);
      }
      Car = mongoose.model('Car', carSchema);
      startSync();
    });
  } catch (err) {
    console.error(err);
  }
})();
