import amqp from "amqplib";
import { type Channel } from "amqplib";


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
  })
  await channel.bindQueue(q.queue, exchange, key);

  return [channel, q]
}