const { Kafka,Partitioners  } = require('kafkajs');
const saslUsername = process.env.kafka_sasl_username;
const saslPassword = process.env.kafka_sasl_password;
const kafkaBroker = process.env.kafka_bootstrap_server;

const kafka = new Kafka({
  clientId: 'kafkajs-producer',
  brokers: [kafkaBroker],
  ssl: true,
  sasl: {
    mechanism: 'plain',
    username: saslUsername,
    password: saslPassword
  },
})
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
const run = async () => {
    // Producing
    let from = ["google","netflix","fb","instagram"];
    let actor=["yashi","susanto","manpreet","vikas"];
    await producer.connect()
    for(let i =0;i<10;i++) {
        const k = from[Math.floor(Math.random() * from.length)];
        const msg = actor[Math.floor(Math.random() * actor.length)];
        await producer.send({
            topic: 'hello-kafka',
            messages: [
              { key:k, value: msg },
            ],
          })
    }
    
  }
  
run().catch('error',console.error)