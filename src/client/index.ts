import amqp from 'amqplib';
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from '../internal/gamelogic/gamelogic.js';
import { SimpleQueueType } from '../internal/pubsub/declareAndBind.js';
import {
  ArmyMovesPrefix,
  ExchangePerilDirect,
  ExchangePerilTopic,
  PauseKey,
  WarRecognitionsPrefix,
} from '../internal/routing/routing.js';
import { GameState } from '../internal/gamelogic/gamestate.js';
import { commandSpawn } from '../internal/gamelogic/spawn.js';
import { commandMove } from '../internal/gamelogic/move.js';
import { subscribeJSON } from '../internal/pubsub/subscribeJSON.js';
import { handlerMove, handlerPause, handlerWar } from './handlers.js';
import { publishJSON } from '../internal/pubsub/publish.js';
import { type ArmyMove, type RecognitionOfWar } from '../internal/gamelogic/gamedata.js';

async function main() {
  console.log('Starting Peril client...');

  const rabbitConnString = 'amqp://guest:guest@localhost:5672/';
  const conn = await amqp.connect(rabbitConnString);
  console.log('Peril game client connected to RabbitMQ!');

  ['SIGINT', 'SIGTERM'].forEach((signal) =>
    process.on(signal, async () => {
      try {
        await conn.close();
        console.log('RabbitMQ connection closed.');
      } catch (err) {
        console.error('Error closing RabbitMQ connection:', err);
      } finally {
        process.exit(0);
      }
    }),
  );

  const username = await clientWelcome();
  const gs = new GameState(username);
  const publishCh = await conn.createConfirmChannel();

  await subscribeJSON(
    conn, 
    ExchangePerilDirect, 
    `${PauseKey}.${username}`,
    PauseKey,
    SimpleQueueType.Transient,
    handlerPause(gs),
  );

  await subscribeJSON<ArmyMove>(
    conn,
    ExchangePerilTopic,
    `${ArmyMovesPrefix}.${username}`,
    `${ArmyMovesPrefix}.*`,
    SimpleQueueType.Transient,
    handlerMove(gs, publishCh),
  );

  await subscribeJSON<RecognitionOfWar>(
    conn,
    ExchangePerilTopic,
    WarRecognitionsPrefix,
    `${WarRecognitionsPrefix}.*`,
    SimpleQueueType.Durable,
    handlerWar(gs),
  );

  while (true) {
    const words = await getInput();
    if (words.length === 0) {
      continue;
    }
    const command = words[0];
    if (command === "move") {
      try {
        const move = commandMove(gs, words);
        await publishJSON(
          publishCh,
          ExchangePerilTopic,
          `${ArmyMovesPrefix}.${username}`,
          move,
        );
        console.log('Move published successfully');
      } catch (err) {
        console.log((err as Error).message);
      }
    } else if (command === "status") {
      commandStatus(gs);
    } else if (command === "spawn") {
      try {
        commandSpawn(gs, words);
      } catch (err) {
        console.log((err as Error).message);
      }
    } else if (command === "help") {
      printClientHelp();
    } else if (command === "quit") {
      printQuit();
      process.exit(0);
    } else if (command === "spam") {
      console.log("Spamming not allowed yet!");
    } else {
      console.log("Unknown command");
      continue;
    }
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
