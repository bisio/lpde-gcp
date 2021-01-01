'use strict';

const express = require('express');
const fetch = require('node-fetch');
var bodyParser = require('body-parser');
const {PubSub} = require('@google-cloud/pubsub');
const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');

let listId = '6d6c146182';
let host   = 'us8.api.mailchimp.com';

const app = express();
app.set('json spaces', 4);
app.use(bodyParser.json());
var apiKey = null;

async function getMailChimpApiKey() {
    const client = new SecretManagerServiceClient();
    const [version] = await client.accessSecretVersion({
        name: 'projects/185146363393/secrets/mailchimp-api-key/versions/latest'
    });

    const payload = version.payload.data.toString();
    apiKey = payload;
}

app.get('/', (req, res) => {
    res.status(200).send('Hello, world!').end();
});

app.post('/merge', async (req, res) => {
    const topicName = "projects/gae-node-666/topics/member-to-process";
    let segmentId = req.body.segmentId;

    let path   = `/3.0/lists/${listId}/segments/${segmentId}/members`;
    const response = await fetch(`https://${host}/${path}`, {
        headers: {
            'Authorization': `apikey ${apiKey}`
        }
    });
 
    const json = await response.json();

    const pubSubClient = new PubSub();

    for (const el of json.members){ 
        await pubSubClient.topic(topicName).publish(Buffer.from(JSON.stringify({
            "id": el.id,
            "email_addresss": el.email_address,
            "merge_fields": el.merge_fields,
            "templatePath": req.body.templatePath,
            "destinationPath": req.body.destinationPath
        })));
    }

    res.status(200).json({
        "status": "done"
    });
});

app.get('/segments', async (req, res) => {
    let path   = `/3.0/lists/${listId}/segments/`;
    const response = await fetch(`https://${host}/${path}`, {
        headers: {
            'Authorization': `apikey ${apiKey}`
        }
    });
    
    const json = await response.json();

    console.info(`Segments: %o`, json);
    
    let segments = json.segments.map(el => { 
        return {
            "id": el.id,
            "name": el.name,
            "member_count":el.member_count
        }
    });

    res.status(200).json({segments:segments});
});

const PORT = process.env.PORT || 8080;

getMailChimpApiKey().then(() => {
    app.listen(PORT, () => {
        console.log(`App listening on port ${PORT}`);
    });
 }
);

module.exports = app;
