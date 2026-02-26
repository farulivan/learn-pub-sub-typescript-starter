import type { ConfirmChannel } from 'amqplib';
import type { ArmyMove, RecognitionOfWar } from '../internal/gamelogic/gamedata.js';
import type { GameState, PlayingState } from '../internal/gamelogic/gamestate.js';
import { handleMove, MoveOutcome } from '../internal/gamelogic/move.js';
import { handlePause } from '../internal/gamelogic/pause.js';
import { handleWar, WarOutcome } from '../internal/gamelogic/war.js';
import { publishJSON } from '../internal/pubsub/publish.js';
import { AckType } from '../internal/pubsub/subscribeJSON.js';
import { ExchangePerilTopic, WarRecognitionsPrefix } from '../internal/routing/routing.js';

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  return (ps: PlayingState): AckType => {
    handlePause(gs, ps);
    process.stdout.write("> ");
    return AckType.Ack;
  }
}

export function handlerMove(gs: GameState, publishCh: ConfirmChannel): (move: ArmyMove) => Promise<AckType> {
  return async (move: ArmyMove): Promise<AckType> => {
    try {
      const outcome = handleMove(gs, move);
      switch (outcome) {
        case MoveOutcome.Safe:
          return AckType.Ack;
        case MoveOutcome.MakeWar: {
          const rw: RecognitionOfWar = {
            attacker: move.player,
            defender: gs.getPlayerSnap(),
          };

          try {
            await publishJSON(
              publishCh,
              ExchangePerilTopic,
              `${WarRecognitionsPrefix}.${gs.getUsername()}`,
              rw,
            );
            return AckType.Ack;
          } catch (err) {
            console.error("Error publishing war recognition:", err);
            return AckType.NackRequeue;
          }
        }
        default:
          return AckType.NackDiscard;
      }
    } finally {
      process.stdout.write("> ");
    }
  };
}

export function handlerWar(gs: GameState): (rw: RecognitionOfWar) => Promise<AckType> {
  return async (rw: RecognitionOfWar): Promise<AckType> => {
    try {
      const outcome = handleWar(gs, rw);
      switch (outcome.result) {
        case WarOutcome.NotInvolved:
          return AckType.NackRequeue;
        case WarOutcome.NoUnits:
          return AckType.NackDiscard;
        case WarOutcome.OpponentWon:
        case WarOutcome.YouWon:
        case WarOutcome.Draw:
          return AckType.Ack;
        default:
          const unreachable: never = outcome;
          console.log("Unexpected war resolution: ", unreachable);
          return AckType.NackDiscard;
      }
    } finally {
      process.stdout.write("> ");
    }
  };
}
