const mqtt = require('mqtt');

const client = mqtt.connect("mqtt://localhost:1883");

client.on('connect', () => {
    console.log('Connected to MQTT Broker');

    client.subscribe('test/topic', (err) => {
        if (!err) {
            console.log('Subscribed successfully');
        }
    });
});

client.on('message', (topic, message) => {
    console.log('Received: ' + message.toString());
});