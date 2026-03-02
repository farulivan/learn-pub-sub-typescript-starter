import amqp from "amqplib";
import { type Channel } from "amqplib";
import { decode } from "@msgpack/msgpack";

export enum AckType {
  Ack = "Ack",
  NackRequeue = "NackRequeue",
  NackDiscard = "NackDiscard",
}

export enum SimpleQueueType {
  Durable,
  Transient,
}

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
  const channel = await conn.createChannel();
  const q = await channel.assertQueue(queueName, {
    durable: queueType === SimpleQueueType.Durable && true,
    autoDelete: queueType === SimpleQueueType.Transient && true,
    exclusive: queueType === SimpleQueueType.Transient && true,
    arguments: {
      'x-dead-letter-exchange': 'peril_dlx'
    },
  })
  await channel.bindQueue(q.queue, exchange, key);

  return [channel, q]
}

export async function subscribe<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  routingKey: string,
  simpleQueueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
  unmarshaller: (data: Buffer) => T,
): Promise<void> {
  const [channel, queueInfo] = await declareAndBind(conn, exchange, queueName, routingKey, simpleQueueType);
  await channel.prefetch(1);
  await channel.consume(queueInfo.queue, async (msg: amqp.ConsumeMessage | null) => {
    if (msg === null) {
      return;
    }
    let data: T;
    try {
      data = unmarshaller(msg.content);
    } catch (err) {
      console.error("Could not unmarshal message:", err);
      return;
    }

    try {
      const result = await handler(data);
      switch (result) {
        case AckType.Ack:
          channel.ack(msg);
          break;
        case AckType.NackDiscard:
          channel.nack(msg, false, false);
          break;
        case AckType.NackRequeue:
          channel.nack(msg, false, true);
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

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  return subscribe<T>(
    conn,
    exchange,
    queueName,
    key,
    queueType,
    handler,
    (data: Buffer) => JSON.parse(data.toString()) as T,
  );
}

export async function subscribeMsgPack<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  return subscribe<T>(
    conn,
    exchange,
    queueName,
    key,
    queueType,
    handler,
    (data: Buffer) => decode(data) as T,
  );
}
