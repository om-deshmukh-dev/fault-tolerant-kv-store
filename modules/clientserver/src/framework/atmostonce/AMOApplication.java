package framework.atmostonce;

import framework.Address;
import framework.Application;
import framework.Command;
import framework.Result;
import java.util.HashMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public final class AMOApplication<T extends Application> implements Application {
  @Getter @NonNull private final T application;
  @Getter @NonNull private HashMap<AMOCommand, AMOResult> executedCommands;
  @Getter @NonNull private HashMap<Address, AMOCommand> clientLatestCommand;

  @Override
  public AMOResult execute(Command command) {
    if (!(command instanceof AMOCommand)) {
      throw new IllegalArgumentException();
    }

    AMOCommand amoCommand = (AMOCommand) command;

    if (this.executedCommands.containsKey(amoCommand)) {
      return this.executedCommands.get(amoCommand);
    }

    if (clientLatestCommand.containsKey(amoCommand.address())) {
      // if the sequence number on the incoming command is smaller,
      // then return garbage because the client will discard this result anyway
      AMOCommand clientLatestAMOCommand = clientLatestCommand.get(amoCommand.address());
      if (amoCommand.sequenceNum() <= clientLatestAMOCommand.sequenceNum()) {
        return executedCommands.get(clientLatestAMOCommand);
      }

      // sequence number in client request is larger, implying the
      // client has sent more recent command; can garbage collect previous one
      executedCommands.remove(clientLatestAMOCommand);
    }

    AMOResult amoResult = new AMOResult(this.application.execute(amoCommand.command()), amoCommand.sequenceNum());
    executedCommands.put(amoCommand, amoResult);
    clientLatestCommand.put(amoCommand.address(), amoCommand);
    return amoResult;
  }

  public Result executeReadOnly(Command command) {
    if (!command.readOnly()) {
      throw new IllegalArgumentException();
    }

    if (command instanceof AMOCommand) {
      return execute(command);
    }

    return application.execute(command);
  }

  public boolean alreadyExecuted(AMOCommand amoCommand) {
    // Your code here...
    return false;
  }
}
