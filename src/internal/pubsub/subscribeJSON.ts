import amqp from "amqplib";
import { declareAndBind, type SimpleQueueType } from './declareAndBind.js';

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType, // an enum to represent "durable" or "transient"
  handler: (data: T) => void,
): Promise<void> {
  const [ channel, queueInfo ] = await declareAndBind(conn, exchange, queueName, key, queueType);
  await channel.consume(queueInfo.queue, (msg) => {
    if (msg === null) {
      return;
    }
    try {
      const content = JSON.parse(msg.content.toString()) as T;
      handler(content);
      channel.ack(msg);
    } catch (err) {
      console.error("Invalid message:", err);
      channel.nack(msg, false, false);
    }
  });
}