package framework.atmostonce;

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
  @Getter @NonNull private HashMap<AMOCommand, AMOResult> alreadyExecuted;

  @Override
  public AMOResult execute(Command command) {
    if (!(command instanceof AMOCommand)) {
      throw new IllegalArgumentException();
    }

    AMOCommand amoCommand = (AMOCommand) command;

    if (this.alreadyExecuted.containsKey(amoCommand)) {
      return this.alreadyExecuted.get(amoCommand);
    }

    AMOResult amoResult = new AMOResult(this.application.execute(amoCommand.command()), amoCommand.sequenceNum());
    alreadyExecuted.put(amoCommand, amoResult);
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
