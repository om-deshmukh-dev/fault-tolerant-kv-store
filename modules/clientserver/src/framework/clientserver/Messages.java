package framework.clientserver;

import framework.atmostonce.AMOCommand;
import framework.atmostonce.AMOResult;
import framework.Command;
import framework.Message;
import framework.Result;
import lombok.Data;

@Data
class Request implements Message {
  private final AMOCommand command;
}

@Data
class Reply implements Message {
  private final AMOResult result;
}
