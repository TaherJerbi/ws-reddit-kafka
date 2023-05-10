import kafka, { Message } from 'kafka-node';
import WebSocket from 'ws';

const client = new kafka.KafkaClient({kafkaHost: 'kafka:9092', connectTimeout: 1000, requestTimeout: 1000});

// retry to connect when kafka is down
const consumer = new kafka.Consumer(client, [{ topic: 'reddit-comments-sentiments', partition: 0 }], { autoCommit: false });

const wss = new WebSocket.Server({ port: 4000 });
const comments: Map<String, Comment> = new Map();

wss.on('connection', function connection(ws: WebSocket) {
    // send last 6 comments
    ws.send(JSON.stringify(Array.from(comments.values()).slice(-6)));
    console.log('new connection');
    ws.on('message', function incoming(message: string) {
        console.log('received: %s', message);
    });
});
consumer.on('message', function (message: Message) {
    const comment = parseComment(message.value.toString());
    comments.set(comment.comment_id, comment);
    console.log('received comment: %s', comment.comment_id);
    wss.clients.forEach(function each(client) {
        if (client.readyState === WebSocket.OPEN) {
            // send last 6 comments
            client.send(JSON.stringify(Array.from(comments.values()).slice(-6)));
        }
    }
    );
});

// comments_id, parent_id, body, subreddit, timestamp, SENTIMENT
type Comment = {
    comment_id: string;
    parent_id: string;
    body: string;
    subreddit: string;
    timestamp: string;
    sentiment: Sentiment;
}
type Sentiment = "Positive" | "Negative" | "Neutral" | "Unknown";

function parseComment(comment: string): Comment {
    const comment_parts = comment.split(',');
    const comment_id = comment_parts[0];
    const parent_id = comment_parts[1];
    // body could contain commas, so we need to join the rest of the array
    const body = comment_parts.slice(2, comment_parts.length - 3).join(',');
    const subreddit = comment_parts[comment_parts.length - 3];
    const timestamp = comment_parts[comment_parts.length - 2];
    const sentiment = comment_parts[comment_parts.length - 1] as Sentiment;

    return {
        comment_id,
        parent_id,
        body,
        subreddit,
        timestamp,
        sentiment
    }
}