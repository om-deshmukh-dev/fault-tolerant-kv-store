package framework.atmostonce;

import framework.Address;
import framework.Application;
import framework.Command;
import framework.Result;
import java.io.Serializable;
import java.util.HashMap;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public final class AMOApplication<T extends Application> implements Application {

  @Data
  public static final class AMOExecution implements Serializable {
    @NonNull private final AMOCommand amoCommand;
    @NonNull private final AMOResult amoResult;
  }

  @Getter @NonNull private final T application;
  @Getter @NonNull private HashMap<Address, AMOExecution> clientLatestExecutedCommand;

  @Override
  public AMOResult execute(Command command) {
    if (!(command instanceof AMOCommand)) {
      throw new IllegalArgumentException();
    }

    AMOCommand amoCommand = (AMOCommand) command;
    AMOExecution amoExecutionLatest;

    if (this.clientLatestExecutedCommand.containsKey(amoCommand.address())) {
      amoExecutionLatest = clientLatestExecutedCommand.get(amoCommand.address());

      // command is stale, and client only cares about latest ongoing request
      // (at most one ongoing request at a time), so return latest one to client
      if (amoCommand.sequenceNum() <= amoExecutionLatest.amoCommand().sequenceNum()) {
        return amoExecutionLatest.amoResult();
      }
    }

    // command has never been executed before, so execute it and store
    // result as latest executed cmd for the client
    Result resultCommand = this.application.execute(amoCommand.command());
    AMOResult amoResult = new AMOResult(resultCommand, amoCommand.sequenceNum());

    clientLatestExecutedCommand.put(amoCommand.address(), new AMOExecution(amoCommand, amoResult));
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
