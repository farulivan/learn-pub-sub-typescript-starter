import amqp from "amqplib";
import { declareAndBind, type SimpleQueueType } from './declareAndBind.js';

export enum AckType {
  Ack = "Ack",
  NackRequeue = "NackRequeue",
  NackDiscard = "NackDiscard",
}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType, // an enum to represent "durable" or "transient"
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  const [ channel, queueInfo ] = await declareAndBind(conn, exchange, queueName, key, queueType);
  await channel.consume(queueInfo.queue, async (msg: amqp.ConsumeMessage | null) => {
    if (msg === null) {
      return;
    }
    let data: T;
    try {
      data = JSON.parse(msg.content.toString());
    } catch (err) {
      console.error("Could not unmarshal message:", err);
      return;
    }

    try {
      const result = await handler(data);
      switch (result) {
        case AckType.Ack:
          channel.ack(msg);
          console.log("Ack");
          break;
        case AckType.NackDiscard:
          channel.nack(msg, false, false);
          console.log("NackDiscard");
          break;
        case AckType.NackRequeue:
          channel.nack(msg, false, true);
          console.log("NackRequeue");
          break;
        default:
          const unreachable: never = result;
          console.error("Unexpected ack type:", unreachable);
          return;
      }
    } catch (err) {
      console.error("Error handling message:", err);
      channel.nack(msg, false, false);
      return;
    }
  });
}