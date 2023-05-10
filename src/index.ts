import kafka, { Message } from 'kafka-node';
import WebSocket from 'ws';

type BatchUpdate = {
    [sentiment in Sentiment]?: number;
}
type SocketMessage = {
    type: "new_comment";
    data: Comment[];
} | {
    type: "batch_update";
    data: BatchUpdate;
}

const client = new kafka.KafkaClient({kafkaHost: 'kafka:9092', connectTimeout: 1000, requestTimeout: 1000});

// retry to connect when kafka is down
const consumer = new kafka.Consumer(client, [{ topic: 'reddit-comments-sentiments', partition: 0 }, { topic: "batch_result", partition: 0 }], { autoCommit: false });

const wss = new WebSocket.Server({ port: 4000 });

const comments: Map<String, Comment> = new Map();
let latestBatchUpdate: BatchUpdate = {};

wss.on('connection', function connection(ws: WebSocket) {
    const commentMessage : SocketMessage = {
        type: "new_comment",
        data: Array.from(comments.values()).slice(-6)
    };

    const batchUpdateMessage : SocketMessage = {
        type: "batch_update",
        data: latestBatchUpdate
    };

    // send last 6 comments
    ws.send(JSON.stringify(commentMessage));
    // send latest batch update
    ws.send(JSON.stringify(batchUpdateMessage));

    console.log('new connection');
    ws.on('message', function incoming(message: string) {
        console.log('received: %s', message);
    });
});
consumer.on('message', function (message: Message) {
    if(message.topic === "reddit-comments-sentiments") {
        handleComment(message.value.toString());
    } else if(message.topic === "batch_result") {
        handleBatchResult(message.value.toString());
    }
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


function handleComment(message: string) {
    const comment = parseComment(message);
    comments.set(comment.comment_id, comment);
    console.log('received comment: %s', comment.comment_id);

    const commentMessage : SocketMessage = {
        type: "new_comment",
        data: Array.from(comments.values()).slice(-6)
    };

    wss.clients.forEach(function each(client) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(commentMessage));
        }
    });
}
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

function handleBatchResult(message: string) {
    const batchUpdate = parseBatchResult(message);

    latestBatchUpdate = batchUpdate;

    const batchUpdateMessage : SocketMessage = {
        type: "batch_update",
        data: batchUpdate
    };
    wss.clients.forEach(function each(client) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(batchUpdateMessage));
        }
    });
}

// [(Positive, 1), (Negative, 2), (Neutral, 3), (Unknown, 4)]
function parseBatchResult(message: string): BatchUpdate {
    const batchUpdate: BatchUpdate = {};

    // remove brackets
    const batchResult = message.slice(1, message.length - 1);
    // remove spaces
    const batchResultNoSpaces = batchResult.replace(/\s/g, '');
    // split by comma
    const batchResultParts = batchResultNoSpaces.split(',');
    for(let i = 0; i < batchResultParts.length; i += 2) {
        const sentiment = batchResultParts[i].slice(1, batchResultParts[i].length) as Sentiment;
        const count = parseInt(batchResultParts[i + 1].slice(0, batchResultParts[i + 1].length - 1));
        batchUpdate[sentiment] = count;
    }
    
    console.log('received batch result: %s', JSON.stringify(batchUpdate));
    
    return batchUpdate;
}