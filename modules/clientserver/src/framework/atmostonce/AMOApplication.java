package framework.atmostonce;

import framework.Application;
import framework.Command;
import framework.Result;
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
  @Getter private int sequenceNum;

  @Override
  public AMOResult execute(Command command) {
    if (!(command instanceof AMOCommand)) {
      throw new IllegalArgumentException();
    }

    AMOCommand amoCommand = (AMOCommand) command;

    // client and server match. therefore client request
    // cannot be stale, and server should execute it
    if (amoCommand.sequenceNum() == this.sequenceNum) {
      Result result = this.application.execute(amoCommand.command());
      this.sequenceNum++;

      // echo back client's sequence number so the client has assurance
      // the server has executed their most recent command
      return new AMOResult(true, result, amoCommand.sequenceNum());
    }

    // client request is stale (sequence numbers do not match).
    return new AMOResult(false, null, this.sequenceNum);
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
